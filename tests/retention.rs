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

fn check_indices(j: &Journal) {
    assert!(j.bytes <= MAX_BYTES);
    assert!(j.records.len() <= MAX_RECORDS);
    assert!(j.components.len() <= j.records.len());
    assert_eq!(
        j.bytes,
        j.records.values().map(|r| Journal::cost(r)).sum::<usize>()
    );
    let mut indexed = 0;
    for (name, component) in &j.components {
        let records: Vec<_> = j
            .records
            .values()
            .filter(|r| r.source.component == *name)
            .collect();
        assert!(!records.is_empty());
        assert!(records.len() <= PER_UNIT_RECORDS);
        assert!(component.bytes <= PER_UNIT_BYTES);
        assert!(component.sequences.capacity() <= 4 * component.sequences.len());
        assert_eq!(
            component.bytes,
            records.iter().map(|r| Journal::cost(r)).sum::<usize>()
        );
        assert!(
            component
                .sequences
                .iter()
                .copied()
                .eq(records.iter().map(|r| r.sequence))
        );
        indexed += records.len();
    }
    assert_eq!(indexed, j.records.len());
}

#[test]
fn indexed_eviction_matches_retention_policy() {
    let mut journal = Journal::default();
    let mut reference: VecDeque<Record> = VecDeque::new();
    let mut bytes = 0;
    let mut discarded = 0;
    let mut random = 7_u32;
    for i in 0..12_000 {
        random = random.wrapping_mul(1664525).wrapping_add(1013904223);
        let mut source = source(&format!(
            "/unit/{}",
            if random & 1 == 0 { 0 } else { random % 23 }
        ));
        source.generation = i / 100;
        source.stream = if i % 2 == 0 {
            Stream::Stdout
        } else {
            Stream::Stderr
        };
        let text = vec![if i % 7 == 0 { 0xff } else { b'a' }; (random as usize % 1000) + 1];
        let decoded = String::from_utf8_lossy(&text).into_owned();
        let cost = decoded.len() + source.component.len() + 128;
        // A simple sequence model verifies both eviction choices independently
        // of the component index, including UTF-8 expansion and generations.
        loop {
            let own: Vec<_> = reference
                .iter()
                .filter(|r| r.source.component == source.component)
                .collect();
            if own.len() < PER_UNIT_RECORDS
                && own.iter().map(|r| Journal::cost(r)).sum::<usize>() + cost <= PER_UNIT_BYTES
            {
                break;
            }
            let position = reference
                .iter()
                .position(|r| r.source.component == source.component);
            assert!(position.is_some());
            if let Some(r) = position.and_then(|p| reference.remove(p)) {
                bytes -= Journal::cost(&r);
                discarded += 1;
            }
        }
        while bytes + cost > MAX_BYTES || reference.len() >= MAX_RECORDS {
            if let Some(r) = reference.pop_front() {
                bytes -= Journal::cost(&r);
                discarded += 1;
            }
        }
        reference.push_back(Record {
            sequence: i + 1,
            monotonic_ns: i,
            source: source.clone(),
            text: decoded,
            continued: false,
        });
        bytes += cost;
        journal.append(&source, &text, false, i);
        if i % 101 == 0 {
            check_indices(&journal);
            assert_eq!(journal.bytes, bytes);
            assert_eq!(journal.discarded, discarded);
            assert!(
                journal
                    .records
                    .keys()
                    .copied()
                    .eq(reference.iter().map(|r| r.sequence))
            );
        }
    }
    check_indices(&journal);
}

#[test]
fn expired_components_leave_no_index_entries() {
    let mut journal = Journal::default();
    for i in 0..MAX_RECORDS * 3 {
        journal.append(&source(&format!("/short-lived/{i}")), b"", false, 0);
    }
    check_indices(&journal);
    assert_eq!(journal.records.len(), MAX_RECORDS);
    assert_eq!(journal.components.len(), MAX_RECORDS);
    assert!(!journal.components.contains_key("/short-lived/0"));
}

#[test]
fn component_index_shrinks_after_global_eviction() {
    let mut journal = Journal::default();
    for _ in 0..PER_UNIT_RECORDS {
        journal.append(&source("busy"), b"", false, 0);
    }
    for i in 0..MAX_RECORDS - 1 {
        journal.append(&source(&format!("/other/{i}")), b"", false, 0);
    }
    assert_eq!(
        journal
            .records
            .values()
            .filter(|r| r.source.component == "busy")
            .count(),
        1
    );
    // Checks both live membership and capacity, not merely the logical length.
    check_indices(&journal);
}

#[test]
fn query_snapshot_survives_concurrent_eviction_without_holding_the_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source("unit");
    let mut journal = Journal::default();
    journal.append(&source, b"old", false, 1);
    let shared = std::sync::Arc::new(std::sync::Mutex::new(journal));
    let (snapshot, retained) = {
        let j = shared.lock().map_err(|_| "journal lock poisoned")?;
        let snapshot = j.snapshot(&Query {
            component: None,
            after: 0,
            through: None,
            limit: 128,
        });
        let retained = std::sync::Arc::downgrade(&snapshot.records[0]);
        (snapshot, retained)
    };
    let writer = shared.clone();
    let worker = std::thread::spawn(move || -> Result<(), &'static str> {
        let mut j = writer.lock().map_err(|_| "writer lock poisoned")?;
        for _ in 0..PER_UNIT_RECORDS + 1 {
            j.append(&source, b"new", false, 2);
        }
        check_indices(&j);
        Ok(())
    });
    worker.join().map_err(|_| "writer panicked")??;
    assert!(retained.upgrade().is_some());
    let page = snapshot.into_page();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].text, "old");
    assert_eq!(page.newest, 1);
    assert_eq!(page.discarded_records, 0);
    // Materializing the reply releases the last journal record reference;
    // clients retain only the bounded wire page, not the retention allocation.
    assert!(retained.upgrade().is_none());
    let j = shared.lock().map_err(|_| "journal lock poisoned")?;
    assert!(j.discarded > 0);
    assert!(j.records.values().all(|r| r.text == "new"));
    Ok(())
}

#[test]
fn empty_or_inverted_cursor_ranges_are_valid() {
    let mut journal = Journal::default();
    journal.append(&source("unit"), b"one", false, 0);
    for (after, through) in [(1, Some(1)), (2, Some(1)), (u64::MAX, None)] {
        let page = journal.query(&Query {
            component: None,
            after,
            through,
            limit: 0,
        });
        assert_eq!(page.next, after);
        assert!(page.records.is_empty());
    }
}

#[test]
fn paginated_queries_preserve_gaps_and_bound_encoded_replies() {
    let mut journal = Journal::default();
    // Maximum admitted name length, JSON escapes and lossy UTF-8 expansion
    // exercise both the retained-byte budget and the larger encoded reply.
    let long_name = format!("/{}", "q\"".repeat(127));
    for i in 0..180 {
        let name = if i % 3 == 0 { "quiet" } else { &long_name };
        let byte = if i % 2 == 0 { 0 } else { 0xff };
        journal.append(&source(name), &[byte; MAX_LINE], false, i);
    }
    let through = journal.sequence - 3;
    for component in [
        None,
        Some(long_name),
        Some("quiet".into()),
        Some("absent".into()),
    ] {
        let expected: Vec<_> = journal
            .records
            .values()
            .filter(|r| {
                r.sequence <= through
                    && component
                        .as_ref()
                        .is_none_or(|name| name == &r.source.component)
            })
            .map(|r| r.sequence)
            .collect();
        let mut actual = Vec::new();
        let mut after = 0;
        for _ in 0..=journal.records.len() {
            let page = journal.query(&Query {
                component: component.clone(),
                after,
                through: Some(through),
                limit: usize::MAX,
            });
            assert!(page.records.len() <= 128);
            assert!(page.records.iter().map(Journal::cost).sum::<usize>() <= 24 * 1024);
            assert!(
                serde_json::to_vec(&page).is_ok_and(|b| b.len() <= hyper_service_ipc::MAX_RESPONSE)
            );
            assert!(page.next <= through);
            actual.extend(page.records.iter().map(|r| r.sequence));
            if page.next == after {
                assert!(page.records.is_empty());
                break;
            }
            assert!(page.next > after);
            after = page.next;
        }
        assert_eq!(actual, expected);
    }
}
