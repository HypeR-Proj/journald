// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn batches_bound_tiny_records_and_preserve_empty_lines() {
    let mut batch = LineBatch::default();
    let mut counts = Vec::new();
    let mut emit = |b: &LineBatch| {
        counts.push(b.records().count());
        assert!(b.mirror_bytes().iter().all(|&b| b == b'\n'));
        assert!(b.records().all(|(text, part)| text.is_empty() && !part));
    };
    for _ in 0..130 {
        batch.push(b"", false, &mut emit);
    }
    batch.flush(&mut emit);
    batch.flush(&mut emit);
    assert_eq!(counts, [64, 64, 2]);
}

#[test]
fn batching_preserves_stream_framing_with_bounded_mirror_messages() {
    let mut batch = LineBatch::default();
    let mut framing = crate::LineBuffer::default();
    let mut records = Vec::new();
    let mut mirror = Vec::new();
    let mut messages = 0;
    let mut emit = |b: &LineBatch| {
        messages += 1;
        assert!(b.mirror_bytes().len() <= MAX_BYTES);
        assert!(b.records().count() <= MAX_RECORDS);
        records.extend(b.records().map(|(text, part)| (text.to_vec(), part)));
        mirror.extend_from_slice(b.mirror_bytes());
    };
    // Each maximum-sized fragment forces the byte quota before the count quota.
    framing.push(&vec![b'x'; crate::MAX_LINE * 9 + 2], 0, |text, part| {
        batch.push(text, part, &mut emit);
    });
    framing.push(b"y\nlast", 1, |text, part| {
        batch.push(text, part, &mut emit)
    });
    framing.flush(|text, part| batch.push(text, part, &mut emit));
    batch.flush(&mut emit);
    assert_eq!(messages, 3);
    assert_eq!(records.len(), 11);
    assert!(
        records[..9]
            .iter()
            .all(|(text, part)| text == &vec![b'x'; crate::MAX_LINE] && *part)
    );
    assert_eq!(records[9], (b"xxy".to_vec(), false));
    assert_eq!(records[10], (b"last".to_vec(), true));
    let expected: Vec<_> = records
        .iter()
        .flat_map(|(text, _)| text.iter().copied().chain([b'\n']))
        .collect();
    assert_eq!(mirror, expected);
    assert!(batch.mirror_bytes().is_empty());
}
