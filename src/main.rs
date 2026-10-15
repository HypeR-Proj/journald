// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Journal ingestion never waits for a console writer or query consumer.

use hyper_journal::{Source, Stream};
use hyper_journald::{Journal, LineBuffer};
use hyper_os::{Error, Status, capability_channel::*, handle::*, wait::*};
use std::{
    mem::MaybeUninit,
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

struct Input {
    source: Source,
    channel: OwnedHandle<ByteChannelObject>,
    lines: LineBuffer,
}

fn record(
    journal: &mut Journal,
    mirror: Option<&OwnedHandle<ByteChannelObject>>,
    source: &Source,
    text: &[u8],
    partial: bool,
    now: u64,
) {
    journal.append(source, text, partial, now);
    if let Some(mirror) = mirror {
        // Reuse per-source line framing for the mirror. Forwarding individual
        // writes here could splice two services' fragments into one log line.
        // Partial/oversized records also end a physical line; retention keeps
        // their continuation metadata. Mirror pressure never blocks ingestion.
        const { assert!(hyper_journald::MAX_LINE < hyper_os::channel::MAX_MESSAGE_BYTES) };
        let mut bytes = Vec::with_capacity(text.len() + 1);
        bytes.extend_from_slice(text);
        bytes.push(b'\n');
        if mirror.as_byte_channel().try_send(&bytes).is_err() {
            journal.mirror_dropped_bytes += bytes.len() as u64;
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut startup = hyper_rt::process::startup()?;
    let listener =
        CapabilityChannel::from_handle(startup.take(hyper_service::console::LOG_CONNECTOR)?);
    let mirror = startup.take_optional(hyper_journal::MIRROR)?;
    let journal = Arc::new(Mutex::new(Journal::default()));
    let outgoing = CapabilityChannel::from_handle(startup.take(hyper_component::OUTGOING)?);
    let shared = journal.clone();
    std::thread::Builder::new()
        .name("journal-query-admit".into())
        .spawn(move || {
            let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            loop {
                let (name, query) = match hyper_component::connection::accept(
                    &outgoing,
                    hyper_os::DEADLINE_INFINITE,
                ) {
                    Ok(c) => c,
                    Err(Error::Status(Status::PEER_CLOSED)) => return,
                    Err(_) => continue,
                };
                if name != "query" || active.load(std::sync::atomic::Ordering::Acquire) >= 8 {
                    continue;
                }
                active.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                let shared = shared.clone();
                let count = active.clone();
                if std::thread::Builder::new()
                    .name("journal-query".into())
                    .spawn(move || {
                        struct Session(Arc<std::sync::atomic::AtomicUsize>);
                        impl Drop for Session {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                            }
                        }
                        let _session = Session(count);
                        loop {
                            let call = match hyper_service_ipc::receive::<hyper_journal::Query>(
                                &query,
                                hyper_os::DEADLINE_INFINITE,
                            ) {
                                Ok(call) => call,
                                Err(Error::Status(Status::PEER_CLOSED)) => return,
                                Err(_) => continue,
                            };
                            let page = match shared.lock() {
                                Ok(j) => j.query(&call.request),
                                Err(_) => return,
                            };
                            let _ = call.respond(&page);
                        }
                    })
                    .is_err()
                {
                    active.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                }
            }
        })?;
    hyper_component::connection::publish(startup.borrow(hyper_component::CONNECTOR)?, "query")?;
    let (tx, rx) = mpsc::sync_channel(16);
    let (wake, reader) = hyper_os::channel::create_pair()?;
    std::thread::Builder::new()
        .name("journal-admit".into())
        .spawn(move || {
            loop {
                let mut bytes = [MaybeUninit::uninit(); 512];
                let mut slots = [CapabilityReceiveSlot::new::<ByteChannelObject>(
                    hyper_journal::SOURCE_RIGHTS,
                )];
                let message =
                    match listener.receive(hyper_os::DEADLINE_INFINITE, &mut bytes, &mut slots) {
                        Ok(m) => m,
                        Err(Error::Status(Status::PEER_CLOSED)) => return,
                        Err(_) => continue,
                    };
                if message.capability_count() != 1 || message.bytes().len() < 5 {
                    continue;
                }
                let b = message.bytes();
                let source = if &b[..4] == b"LOG2" {
                    serde_json::from_slice::<Source>(&b[4..]).ok()
                } else if &b[..4] == b"LOGS" {
                    std::str::from_utf8(&b[4..]).ok().map(|s| Source {
                        component: s.into(),
                        generation: 0,
                        stream: Stream::Log,
                    })
                } else {
                    None
                };
                let Some(source) = source.filter(|s| {
                    !s.component.is_empty()
                        && s.component.len() <= 255
                        && !s.component.chars().any(char::is_control)
                }) else {
                    continue;
                };
                let Ok(Some(channel)) = slots[0].take() else {
                    continue;
                };
                if tx
                    .send(Input {
                        source,
                        channel,
                        lines: LineBuffer::default(),
                    })
                    .is_err()
                {
                    return;
                }
                let _ = wake.as_byte_channel().try_send(&[1]);
            }
        })?;
    hyper_component::notify(&startup, &hyper_component::Notification::Ready)?;
    let mut inputs: Vec<Input> = Vec::new();
    let mut scratch = vec![0; hyper_os::channel::MAX_MESSAGE_BYTES];
    let mut last_flush = 0;
    loop {
        while reader.as_byte_channel().try_receive(&mut scratch).is_ok() {}
        for input in rx.try_iter() {
            if inputs.len() < 60 {
                inputs.push(input);
            } else if let Ok(mut j) = journal.lock() {
                j.rejected_sources += 1;
            }
        }
        let now = hyper_os::time::monotonic_now()?.as_nanoseconds();
        let flush_partial = now.saturating_sub(last_flush) >= 100_000_000;
        if flush_partial {
            last_flush = now;
        }
        inputs.retain_mut(|input| {
            let mut closed = false;
            // Per-source budget prevents one producer from starving siblings.
            for _ in 0..8 {
                match input.channel.as_byte_channel().try_receive(&mut scratch) {
                    Ok(n) => {
                        if let Ok(mut j) = journal.lock() {
                            input.lines.push(&scratch[..n], |text, part| {
                                record(&mut j, mirror.as_ref(), &input.source, text, part, now)
                            });
                        }
                    }
                    Err(Error::Status(Status::WOULD_BLOCK)) => break,
                    Err(_) => {
                        closed = true;
                        break;
                    }
                }
            }
            // Flush partial lines periodically so unterminated diagnostics are visible.
            if (closed || flush_partial)
                && let Ok(mut j) = journal.lock()
            {
                input.lines.flush(|text, part| {
                    record(&mut j, mirror.as_ref(), &input.source, text, part, now)
                });
            }
            !closed
        });
        let mut waits = vec![WaitItem::new(
            reader.as_handle_ref(),
            ObjectSignals::<ByteChannelObject>::READABLE,
        )];
        for input in &inputs {
            waits.push(WaitItem::new(
                input.channel.as_handle_ref(),
                ObjectSignals::<ByteChannelObject>::READABLE
                    .union(ObjectSignals::<ByteChannelObject>::PEER_CLOSED),
            ));
        }
        match wait_many(
            &waits,
            hyper_os::time::deadline_after(Duration::from_millis(100))?.as_raw(),
        ) {
            Ok(_) | Err(Error::Status(Status::TIMED_OUT)) => {}
            Err(e) => return Err(e.into()),
        }
    }
}
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("HypeR journal: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
