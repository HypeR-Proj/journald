// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use std::ops::Range;

const MAX_BYTES: usize = 16 * 1024;
const MAX_RECORDS: usize = 64;

/// One source's bounded, line-framed ingestion batch. Flush before changing
/// sources. Parsing and mirror writes happen outside the shared retention lock.
pub struct LineBatch {
    bytes: Vec<u8>,
    lines: Vec<(Range<usize>, bool)>,
}

impl Default for LineBatch {
    fn default() -> Self {
        Self {
            bytes: Vec::with_capacity(MAX_BYTES),
            lines: Vec::with_capacity(MAX_RECORDS),
        }
    }
}

impl LineBatch {
    pub const MAX_BYTES: usize = MAX_BYTES;

    pub fn push(&mut self, text: &[u8], continued: bool, emit: impl FnMut(&Self)) {
        let text = &text[..text.len().min(super::MAX_LINE)];
        if self.lines.len() == MAX_RECORDS || self.bytes.len() + text.len() + 1 > MAX_BYTES {
            self.flush(emit);
        }
        let start = self.bytes.len();
        self.bytes.extend_from_slice(text);
        self.lines.push((start..self.bytes.len(), continued));
        self.bytes.push(b'\n');
    }

    pub fn flush(&mut self, mut emit: impl FnMut(&Self)) {
        if !self.lines.is_empty() {
            emit(self);
            self.lines.clear();
            self.bytes.clear();
        }
    }

    pub fn records(&self) -> impl Iterator<Item = (&[u8], bool)> {
        self.lines
            .iter()
            .map(|(range, part)| (&self.bytes[range.clone()], *part))
    }

    /// Complete framed lines, suitable for one nonblocking mirror write.
    pub fn mirror_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
#[path = "../tests/batch.rs"]
mod tests;
