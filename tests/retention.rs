// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hyper_journal::Stream;
fn source(name: &str) -> Source {
    Source {
        component: name.into(),
        generation: 1,
        stream: Stream::Stderr,
    }
}

#[test]
fn a_noisy_source_does_not_evict_a_quiet_service() {
    let mut j = Journal::default();
    j.append(&source("quiet"), b"important", false, 1);
    for i in 0..10_000 {
        j.append(&source("noisy"), b"a", false, i);
    }
    assert!(j.bytes <= MAX_BYTES);
    assert!(j.records.len() <= MAX_RECORDS);
    let page = j.query(&Query {
        component: Some("quiet".into()),
        through: None,
        after: 0,
        limit: 100,
    });
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].text, "important");
    assert!(page.discarded_records > 0);
}

#[test]
fn unterminated_lines_are_split_and_memory_remains_bounded() {
    let mut lines = LineBuffer::default();
    let mut sizes = Vec::new();
    lines.push(&vec![b'x'; MAX_LINE * 3 + 2], 1, |b, partial| {
        assert!(partial);
        sizes.push(b.len());
    });
    assert_eq!(sizes, vec![MAX_LINE; 3]);
    assert_eq!(lines.pending.len(), 2);
    lines.push(b"\n", 2, |b, partial| {
        assert!(!partial);
        assert_eq!(b, b"xx");
    });
    assert!(lines.pending.is_empty());
}

#[test]
fn filtered_cursor_advances_without_duplicate_records() {
    let mut j = Journal::default();
    for i in 0..10 {
        j.append(&source(if i % 2 == 0 { "a" } else { "b" }), b"x", false, i);
    }
    let p = j.query(&Query {
        component: Some("a".into()),
        through: None,
        after: 0,
        limit: 2,
    });
    assert_eq!(p.records.len(), 2);
    let q = j.query(&Query {
        component: Some("a".into()),
        through: None,
        after: p.next,
        limit: 128,
    });
    assert_eq!(q.records.len(), 3);
    assert_eq!(q.next, 10);
}

#[test]
fn bounded_query_does_not_chase_new_records_between_pages() {
    let mut j = Journal::default();
    for i in 0..5 {
        j.append(&source("a"), b"old", false, i);
    }
    let first = j.query(&Query {
        component: None,
        through: None,
        after: 0,
        limit: 2,
    });
    for i in 5..20 {
        j.append(&source("a"), b"new", false, i);
    }
    let rest = j.query(&Query {
        component: None,
        through: Some(first.newest),
        after: first.next,
        limit: 128,
    });
    assert_eq!(rest.next, first.newest);
    assert_eq!(rest.records.len(), 3);
    assert!(rest.records.iter().all(|r| r.text == "old"));
}

#[test]
fn stream_fragments_preserve_lines_and_flush_once_at_eof() {
    let mut lines = LineBuffer::default();
    let mut output = Vec::new();
    let mut emit = |b: &[u8], partial| output.push((b.to_vec(), partial));
    lines.push(b"first", 1, &mut emit);
    lines.push(b" line\nsecond", 2, &mut emit);
    lines.flush(&mut emit);
    lines.flush(&mut emit);
    assert_eq!(
        output,
        vec![(b"first line".to_vec(), false), (b"second".to_vec(), true)]
    );
}

#[test]
fn retirement_record_survives_a_flush_tick_between_fragments() {
    let mut lines = LineBuffer::default();
    let mut output = Vec::new();
    let mut emit = |b: &[u8], partial| output.push((b.to_vec(), partial));
    // CI received this prefix just as the old global flush timer expired.
    lines.push(
        b"HypeR IO VM: HypeR I/O [hbr0]: rep",
        100_000_000,
        &mut emit,
    );
    lines.flush_idle(100_000_000, &mut emit);
    lines.push(b"ly RELEASE_MEMORY: ok\n", 101_000_000, &mut emit);
    lines.flush_idle(201_000_000, &mut emit);
    assert_eq!(
        output,
        vec![(
            b"HypeR IO VM: HypeR I/O [hbr0]: reply RELEASE_MEMORY: ok".to_vec(),
            false
        )]
    );
}

#[test]
fn partial_flush_waits_for_each_sources_idle_period() {
    let mut busy = LineBuffer::default();
    let mut quiet = LineBuffer::default();
    let mut output = Vec::new();
    let mut emit = |b: &[u8], partial| output.push((b.to_vec(), partial));
    busy.push(b"first", 1, &mut emit);
    quiet.push(b"quiet", 1, &mut emit);
    busy.push(b" second", 90_000_000, &mut emit);
    busy.flush_idle(100_000_001, &mut emit);
    quiet.flush_idle(100_000_001, &mut emit);
    // Empty channel messages must not keep an unterminated line hidden.
    busy.push(b"", 180_000_000, &mut emit);
    busy.flush_idle(189_999_999, &mut emit);
    assert_eq!(busy.pending, b"first second");
    busy.flush_idle(190_000_000, &mut emit);
    busy.flush_idle(290_000_000, &mut emit);
    assert_eq!(
        output,
        vec![(b"quiet".to_vec(), true), (b"first second".to_vec(), true)]
    );
}
