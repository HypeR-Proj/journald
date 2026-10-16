// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded volatile retention. Both total bytes and record count are bounded;
//! a noisy service cannot evict every other service through tiny records.

use hyper_journal::{Page, Query, Record, Source};
use std::{
    collections::{BTreeMap, VecDeque},
    ops::Bound::{Excluded, Included},
    sync::Arc,
};

mod batch;
pub use batch::LineBatch;

pub const MAX_LINE: usize = 4096;
const MAX_BYTES: usize = 1024 * 1024;
const MAX_RECORDS: usize = 4096;
const PER_UNIT_BYTES: usize = 128 * 1024;
const PER_UNIT_RECORDS: usize = 512;

#[derive(Default)]
struct ComponentRetention {
    sequences: VecDeque<u64>,
    bytes: usize,
}

#[derive(Default)]
pub struct Journal {
    // Both indices contain only retained records, including across restarts.
    records: BTreeMap<u64, Arc<Record>>,
    components: BTreeMap<String, ComponentRetention>,
    bytes: usize,
    sequence: u64,
    discarded: u64,
    pub rejected_sources: u64,
    pub mirror_dropped_bytes: u64,
}

/// Bounded references selected under the retention lock. Materialize the wire
/// reply after releasing that lock; slow clients must not stall ingestion.
pub struct PageSnapshot {
    records: Vec<Arc<Record>>,
    page: Page,
}

impl PageSnapshot {
    pub fn into_page(self) -> Page {
        Page {
            records: self.records.into_iter().map(Arc::unwrap_or_clone).collect(),
            ..self.page
        }
    }
}

impl Journal {
    pub fn append(&mut self, source: &Source, text: &[u8], continued: bool, now: u64) {
        // Lossy decoding expands by at most 3; size accounting uses stored UTF-8.
        let text = String::from_utf8_lossy(&text[..text.len().min(MAX_LINE)]).into_owned();
        let cost = text.len() + source.component.len() + 128;
        while let Some(component) = self.components.get(&source.component) {
            if component.sequences.len() < PER_UNIT_RECORDS
                && component.bytes + cost <= PER_UNIT_BYTES
            {
                break;
            }
            let Some(&sequence) = component.sequences.front() else {
                break;
            };
            self.evict(sequence);
        }
        while self.bytes + cost > MAX_BYTES || self.records.len() >= MAX_RECORDS {
            let Some((&sequence, _)) = self.records.first_key_value() else {
                break;
            };
            self.evict(sequence);
        }
        self.sequence += 1;
        self.bytes += cost;
        if !self.components.contains_key(&source.component) {
            self.components
                .insert(source.component.clone(), ComponentRetention::default());
        }
        if let Some(component) = self.components.get_mut(&source.component) {
            component.bytes += cost;
            component.sequences.push_back(self.sequence);
        }
        self.records.insert(
            self.sequence,
            Arc::new(Record {
                sequence: self.sequence,
                monotonic_ns: now,
                source: source.clone(),
                text,
                continued,
            }),
        );
    }

    /// Every eviction removes the oldest retained record of its component,
    /// whether chosen by that component's quota or by the global quota.
    fn evict(&mut self, sequence: u64) {
        let Some(record) = self.records.remove(&sequence) else {
            return;
        };
        let cost = Self::cost(&record);
        self.bytes -= cost;
        self.discarded += 1;
        if let Some(component) = self.components.get_mut(&record.source.component) {
            component.sequences.pop_front();
            component.bytes -= cost;
            if component.sequences.is_empty() {
                self.components.remove(&record.source.component);
            } else if component.sequences.capacity() > 4 * component.sequences.len() {
                // A once-busy component must not retain its peak index capacity
                // indefinitely after global eviction leaves only a few records.
                component.sequences.shrink_to(2 * component.sequences.len());
            }
        }
    }

    fn cost(r: &Record) -> usize {
        r.text.len() + r.source.component.len() + 128
    }

    pub fn snapshot(&self, q: &Query) -> PageSnapshot {
        let mut records = Vec::new();
        let mut bytes = 0;
        let mut next = q.after;
        let through = q.through.unwrap_or(u64::MAX);
        // BTreeMap rejects an inverted range. Such a cursor simply has no data.
        if q.after < through {
            for (&sequence, record) in self.records.range((Excluded(q.after), Included(through))) {
                if q.component
                    .as_ref()
                    .is_some_and(|u| u != &record.source.component)
                {
                    next = sequence;
                    continue;
                }
                if records.len() >= q.limit.clamp(1, 128) || bytes + Self::cost(record) > 24 * 1024
                {
                    break;
                }
                next = sequence;
                bytes += Self::cost(record);
                records.push(Arc::clone(record));
            }
        }
        PageSnapshot {
            records,
            page: Page {
                records: Vec::new(),
                next,
                oldest: self
                    .records
                    .first_key_value()
                    .map_or(self.sequence + 1, |(&sequence, _)| sequence),
                newest: self.sequence,
                discarded_records: self.discarded,
                rejected_sources: self.rejected_sources,
                mirror_dropped_bytes: self.mirror_dropped_bytes,
            },
        }
    }

    pub fn query(&self, q: &Query) -> Page {
        self.snapshot(q).into_page()
    }
}

#[cfg(test)]
#[path = "../tests/retention.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/throughput.rs"]
mod throughput;

#[derive(Default)]
pub struct LineBuffer {
    pending: Vec<u8>,
    last_input_ns: u64,
}
impl LineBuffer {
    pub fn push(&mut self, bytes: &[u8], now: u64, mut emit: impl FnMut(&[u8], bool)) {
        if !bytes.is_empty() {
            self.last_input_ns = now;
        }
        for &b in bytes {
            if b == b'\n' {
                emit(&self.pending, false);
                self.pending.clear();
            } else {
                self.pending.push(b);
                if self.pending.len() == MAX_LINE {
                    emit(&self.pending, true);
                    self.pending.clear();
                }
            }
        }
    }
    /// Give each source time to complete a line across separate channel writes.
    /// A global periodic flush can split a fragment immediately after receipt.
    pub fn flush_idle(&mut self, now: u64, emit: impl FnMut(&[u8], bool)) {
        if now.saturating_sub(self.last_input_ns) >= 100_000_000 {
            self.flush(emit);
        }
    }
    pub fn flush(&mut self, mut emit: impl FnMut(&[u8], bool)) {
        if !self.pending.is_empty() {
            emit(&self.pending, true);
            self.pending.clear();
        }
    }
}
