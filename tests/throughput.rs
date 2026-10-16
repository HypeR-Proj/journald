// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hyper_journal::Stream;
use std::{hint::black_box, time::Instant};

/// Host retention cost only: excludes channel transport, scheduling and serial I/O.
#[test]
#[ignore = "manual release-mode measurement; never a CI performance threshold"]
fn retention_throughput() {
    const ITERATIONS: u64 = 50_000;
    for (components, query_every) in [(1, 0), (16, 0), (16, 32)] {
        let sources: Vec<_> = (0..components)
            .map(|i| Source {
                component: format!("/services/producer-{i}"),
                generation: 1,
                stream: Stream::Stdout,
            })
            .collect();
        let mut samples = Vec::new();
        for _ in 0..3 {
            let mut journal = Journal::default();
            let payload = [b'x'; 128];
            for i in 0..8192 {
                journal.append(&sources[i % components], &payload, false, i as u64);
            }
            let start = Instant::now();
            for i in 0..ITERATIONS {
                journal.append(&sources[i as usize % components], &payload, false, i);
                if query_every != 0 && i % query_every == 0 {
                    black_box(journal.query(&Query {
                        component: None,
                        after: journal.sequence.saturating_sub(128),
                        through: None,
                        limit: 128,
                    }));
                }
            }
            samples.push(start.elapsed().as_nanos() / ITERATIONS as u128);
            black_box(journal);
        }
        samples.sort_unstable();
        println!(
            "journal components={components} query_every={query_every}: median={} ns/append; samples={samples:?}",
            samples[1]
        );
    }
}
