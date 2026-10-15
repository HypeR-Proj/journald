// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Bounded volatile retention. Both total bytes and record count are bounded;
//! a noisy service cannot evict every other service through tiny records.

use hyper_journal::{Page, Query, Record, Source};
use std::collections::VecDeque;

pub const MAX_LINE: usize = 4096;
const MAX_BYTES: usize = 1024 * 1024;
const MAX_RECORDS: usize = 4096;
const PER_UNIT_BYTES: usize = 128 * 1024;
const PER_UNIT_RECORDS: usize = 512;

#[derive(Default)]
pub struct Journal {
    records: VecDeque<Record>,
    bytes: usize,
    sequence: u64,
    discarded: u64,
    pub rejected_sources: u64,
    pub mirror_dropped_bytes: u64,
}

impl Journal {
    pub fn append(&mut self, source: &Source, text: &[u8], continued: bool, now: u64) {
        // Lossy decoding expands by at most 3; size accounting uses stored UTF-8.
        let text = String::from_utf8_lossy(&text[..text.len().min(MAX_LINE)]).into_owned();
        let cost = text.len() + source.component.len() + 128;
        let mut count = 0;
        let mut bytes = 0;
        for r in &self.records {
            if r.source.component == source.component {
                count += 1;
                bytes += Self::cost(r);
            }
        }
        while count >= PER_UNIT_RECORDS || bytes + cost > PER_UNIT_BYTES {
            let Some(i) = self
                .records
                .iter()
                .position(|r| r.source.component == source.component)
            else {
                break;
            };
            if let Some(r) = self.records.remove(i) {
                let size = Self::cost(&r);
                self.bytes -= size;
                bytes -= size;
                count -= 1;
                self.discarded += 1;
            }
        }
        while self.bytes + cost > MAX_BYTES || self.records.len() >= MAX_RECORDS {
            let Some(r) = self.records.pop_front() else {
                break;
            };
            self.bytes -= Self::cost(&r);
            self.discarded += 1;
        }
        self.sequence += 1;
        self.bytes += cost;
        self.records.push_back(Record {
            sequence: self.sequence,
            monotonic_ns: now,
            source: source.clone(),
            text,
            continued,
        });
    }
    fn cost(r: &Record) -> usize {
        r.text.len() + r.source.component.len() + 128
    }

    pub fn query(&self, q: &Query) -> Page {
        let mut records = Vec::new();
        let mut bytes = 0;
        let mut next = q.after;
        for r in self
            .records
            .iter()
            .filter(|r| r.sequence > q.after && r.sequence <= q.through.unwrap_or(u64::MAX))
        {
            if q.component
                .as_ref()
                .is_some_and(|u| u != &r.source.component)
            {
                next = r.sequence;
                continue;
            }
            if records.len() >= q.limit.clamp(1, 128) || bytes + Self::cost(r) > 24 * 1024 {
                break;
            }
            next = r.sequence;
            bytes += Self::cost(r);
            records.push(r.clone());
        }
        Page {
            records,
            next,
            oldest: self
                .records
                .front()
                .map_or(self.sequence + 1, |r| r.sequence),
            newest: self.sequence,
            discarded_records: self.discarded,
            rejected_sources: self.rejected_sources,
            mirror_dropped_bytes: self.mirror_dropped_bytes,
        }
    }
}

#[cfg(test)]
#[path = "../tests/retention.rs"]
mod tests;

#[derive(Default)]
pub struct LineBuffer {
    pending: Vec<u8>,
}
impl LineBuffer {
    pub fn push(&mut self, bytes: &[u8], mut emit: impl FnMut(&[u8], bool)) {
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
    pub fn flush(&mut self, mut emit: impl FnMut(&[u8], bool)) {
        if !self.pending.is_empty() {
            emit(&self.pending, true);
            self.pending.clear();
        }
    }
}
