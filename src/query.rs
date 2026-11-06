// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Fixed request workers and bounded, expiring endpoint delegation.

use hyper_journald::Journal;
use hyper_os::capability_channel::{CapabilityChannel, CapabilityDisposition};
use hyper_os::handle::RightsOffer;
use hyper_os::{Error, Result, Status};
use hyper_service_ipc::Admission;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration;

const WORKERS: usize = 8;
const HANDOFFS: usize = 8;
const SLICE: Duration = Duration::from_millis(250);
const REPLY_BUDGET: Duration = Duration::from_secs(5);

pub(super) struct Service {
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl Service {
    pub(super) fn start(outgoing: CapabilityChannel, journal: Arc<Mutex<Journal>>) -> Result<Self> {
        let (requests, client) = CapabilityChannel::create()?;
        let requests = Arc::new(requests);
        let client = Arc::new(client);
        let mut service = Self {
            stop: Arc::new(AtomicBool::new(false)),
            workers: Vec::new(),
        };
        for _ in 0..WORKERS {
            let requests = requests.clone();
            let journal = journal.clone();
            let stop = service.stop.clone();
            service.workers.push(
                std::thread::Builder::new()
                    .name("journal-query".into())
                    .spawn(move || {
                        if let Err(error) = serve(&requests, &journal, &stop) {
                            eprintln!("journal query worker stopped: {error}");
                        }
                    })
                    .map_err(|_| Error::Status(Status::NO_MEMORY))?,
            );
        }
        let stop = service.stop.clone();
        service.workers.push(
            std::thread::Builder::new()
                .name("journal-query-admit".into())
                .spawn(move || admit(outgoing, client, stop))
                .map_err(|_| Error::Status(Status::NO_MEMORY))?,
        );
        Ok(service)
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            if worker.join().is_err() {
                eprintln!("journal query worker terminated unexpectedly");
            }
        }
    }
}

fn serve(requests: &CapabilityChannel, journal: &Mutex<Journal>, stop: &AtomicBool) -> Result<()> {
    let mut reply_failures = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let until = hyper_os::time::deadline_after(SLICE)?.as_raw();
        let call = match hyper_service_ipc::receive::<hyper_journal::Query>(requests, until) {
            Ok(Admission::Accepted(call)) => call,
            Ok(Admission::Rejected(_)) | Err(Error::Status(Status::TIMED_OUT)) => continue,
            Err(Error::Status(Status::PEER_CLOSED)) => return Ok(()),
            Err(error) => return Err(error),
        };
        let snapshot = journal
            .lock()
            .map_err(|_| Error::Status(Status::INTERNAL))?
            .snapshot(&call.request);
        // Retention ownership ends before bounded reply I/O. Every request
        // carries its own reply channel; an idle client owns no server worker.
        let until = hyper_os::time::deadline_after(REPLY_BUDGET)?.as_raw();
        if let Err(error) = call.respond_until(&snapshot.into_page(), until)
            && !error.is_reply_abandoned()
        {
            reply_failures = reply_failures.saturating_add(1);
            if reply_failures.is_power_of_two() {
                eprintln!("journal query reply failure #{reply_failures}: {error}");
            }
        }
    }
    Ok(())
}

fn admit(outgoing: CapabilityChannel, client: Arc<CapabilityChannel>, stop: Arc<AtomicBool>) {
    let mut handoffs: Vec<JoinHandle<()>> = Vec::new();
    let result = (|| -> Result<()> {
        while !stop.load(Ordering::Acquire) {
            let mut index = 0;
            while index < handoffs.len() {
                if handoffs[index].is_finished() {
                    handoffs
                        .swap_remove(index)
                        .join()
                        .map_err(|_| Error::Status(Status::INTERNAL))?;
                } else {
                    index += 1;
                }
            }
            let until = hyper_os::time::deadline_after(SLICE)?.as_raw();
            let (name, session) = match hyper_component::connection::accept(&outgoing, until) {
                Ok(Admission::Accepted(connection)) => connection,
                Ok(Admission::Rejected(_)) | Err(Error::Status(Status::TIMED_OUT)) => continue,
                Err(Error::Status(Status::PEER_CLOSED)) => return Ok(()),
                Err(error) => return Err(error),
            };
            if name != "query" || handoffs.len() == HANDOFFS {
                continue;
            }
            let limit = hyper_service_ipc::deadline()?;
            let client = client.clone();
            let canceled = stop.clone();
            match std::thread::Builder::new()
                .name("journal-query-handoff".into())
                .spawn(move || {
                    if let Err(error) = delegate(&session, &client, limit, &canceled)
                        && !matches!(
                            error,
                            Error::Status(
                                Status::PEER_CLOSED | Status::TIMED_OUT | Status::CANCELLED
                            )
                        )
                    {
                        eprintln!("journal query delegation failed: {error}");
                    }
                }) {
                Ok(worker) => handoffs.push(worker),
                Err(error) => eprintln!("journal query delegation worker unavailable: {error}"),
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        eprintln!("journal query admission listener stopped: {error}");
    }
    // Existing query endpoints remain usable if only new admission has failed.
    // Each pending delegation has its own original finite deadline.
    for worker in handoffs {
        if worker.join().is_err() {
            eprintln!("journal query delegation worker terminated unexpectedly");
        }
    }
}

fn delegate(
    session: &CapabilityChannel,
    client: &CapabilityChannel,
    limit: u64,
    stop: &AtomicBool,
) -> Result<()> {
    while !stop.load(Ordering::Acquire) {
        let until = hyper_os::time::deadline_after(SLICE)?.as_raw().min(limit);
        let result = hyper_service_ipc::send(
            session,
            until,
            hyper_journal::QUERY_READY,
            &mut [CapabilityDisposition::duplicate(
                client.as_handle_ref(),
                RightsOffer::Exact(hyper_service_ipc::CLIENT_RIGHTS),
            )?],
        );
        match result {
            Err(Error::Status(Status::TIMED_OUT)) if until < limit => continue,
            result => return result,
        }
    }
    Ok(())
}
