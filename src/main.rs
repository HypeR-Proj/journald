// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded journal ingestion; console and query I/O run outside the retention lock.

use hyper_journal::{Source, Stream};
use hyper_journald::{Journal, LineBatch, LineBuffer};
use hyper_os::{Error, Status, capability_channel::*, handle::*, wait::*};
use std::{
    mem::MaybeUninit,
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

const ADMISSION_BUDGET: usize = 16;

struct Input {
    source: Source,
    channel: OwnedHandle<ByteChannelObject>,
    lines: LineBuffer,
}

fn record_batch(
    journal: &Mutex<Journal>,
    mirror: Option<&OwnedHandle<ByteChannelObject>>,
    source: &Source,
    batch: &LineBatch,
    now: u64,
) {
    if let Ok(mut journal) = journal.lock() {
        for (text, partial) in batch.records() {
            journal.append(source, text, partial, now);
        }
    }
    if let Some(mirror) = mirror {
        // Reuse per-source line framing for the mirror. Forwarding individual
        // writes here could splice two services' fragments into one log line.
        // Partial/oversized records also end a physical line; retention keeps
        // their continuation metadata. Mirror pressure never blocks ingestion.
        const { assert!(LineBatch::MAX_BYTES <= hyper_os::channel::MAX_MESSAGE_BYTES) };
        let bytes = batch.mirror_bytes();
        if mirror.as_byte_channel().try_send(bytes).is_err()
            && let Ok(mut journal) = journal.lock()
        {
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
                            let snapshot = match shared.lock() {
                                Ok(j) => j.snapshot(&call.request),
                                Err(_) => return,
                            };
                            let _ = call.respond(&snapshot.into_page());
                        }
                    })
                    .is_err()
                {
                    active.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                }
            }
        })?;
    hyper_component::connection::publish(startup.borrow(hyper_component::CONNECTOR)?, "query")?;
    let (tx, rx) = mpsc::sync_channel(ADMISSION_BUDGET);
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
    let mut batch = LineBatch::default();
    loop {
        // The producer can refill either queue while we drain it. Bound both
        // loops so reconnect churn cannot indefinitely postpone existing logs.
        for _ in 0..ADMISSION_BUDGET {
            if reader.as_byte_channel().try_receive(&mut scratch).is_err() {
                break;
            }
        }
        let mut admitted = 0;
        for input in rx.try_iter().take(ADMISSION_BUDGET) {
            admitted += 1;
            if inputs.len() < 60 {
                inputs.push(input);
            } else if let Ok(mut j) = journal.lock() {
                j.rejected_sources += 1;
            }
        }
        let now = hyper_os::time::monotonic_now()?.as_nanoseconds();
        inputs.retain_mut(|input| {
            let mut closed = false;
            let mut received = 0;
            let emit_batch = |batch: &LineBatch| {
                record_batch(&journal, mirror.as_ref(), &input.source, batch, now)
            };
            // Bound bytes as well as messages: eight full messages used to
            // process 512 KiB from one producer before visiting its siblings.
            for _ in 0..8 {
                match input.channel.as_byte_channel().try_receive(&mut scratch) {
                    Ok(n) => {
                        input.lines.push(&scratch[..n], now, |text, part| {
                            batch.push(text, part, emit_batch);
                        });
                        received += n;
                        if received >= hyper_os::channel::MAX_MESSAGE_BYTES {
                            break;
                        }
                    }
                    Err(Error::Status(Status::WOULD_BLOCK)) => break,
                    Err(_) => {
                        closed = true;
                        break;
                    }
                }
            }
            // Only idle sources flush partial lines. A busy source may still
            // have a line's remaining fragments queued after its read budget.
            let emit = |text: &[u8], part| batch.push(text, part, emit_batch);
            if closed {
                input.lines.flush(emit);
            } else {
                input.lines.flush_idle(now, emit);
            }
            batch.flush(emit_batch);
            !closed
        });
        // Some queued inputs may correspond to wake messages already consumed.
        // Revisit them without sleeping, but only after servicing active inputs.
        if admitted == ADMISSION_BUDGET {
            continue;
        }
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
