//! Rolled-up per-prefix aggregates.
//!
//! Keys are slash-separated paths. A put of key `a/b/c` updates the counters
//! of every prefix of the key: `""`, `a`, and `a/b`. Queries are a single map
//! lookup (hashing cost is O(depth) in the prefix length); no object scan is
//! ever performed.

use std::collections::HashMap;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Aggregate {
    pub object_count: u64,
    pub byte_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AggDeltaEntry {
    pub prefix: String,
    pub object_count_delta: u64,
    pub byte_count_delta: u64,
}

fn prefixes_of(key: &str) -> Vec<String> {
    let mut out = vec![String::new()];
    let mut prefix = String::new();
    for part in key.split('/').filter(|s| !s.is_empty()) {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(part);
        out.push(prefix.clone());
    }
    out
}

#[derive(Debug, Default)]
pub struct Rollup {
    aggregates: HashMap<String, Aggregate>,
}

impl Rollup {
    pub fn record(&mut self, key: &str, len: u64) {
        for prefix in prefixes_of(key) {
            let agg = self.aggregates.entry(prefix).or_default();
            agg.object_count += 1;
            agg.byte_count += len;
        }
    }

    pub fn deltas_for(key: &str, len: u64) -> Vec<AggDeltaEntry> {
        prefixes_of(key)
            .into_iter()
            .map(|prefix| AggDeltaEntry {
                prefix,
                object_count_delta: 1,
                byte_count_delta: len,
            })
            .collect()
    }

    pub fn get(&self, prefix: &str) -> Aggregate {
        self.aggregates.get(prefix).copied().unwrap_or_default()
    }
}
