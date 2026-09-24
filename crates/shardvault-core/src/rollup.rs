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
    let parts: Vec<&str> = key.split('/').filter(|s| !s.is_empty()).collect();
    let mut out = vec![String::new()];
    let mut prefix = String::new();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            prefix.push('/');
        }
        prefix.push_str(part);
        if i + 1 < parts.len() {
            out.push(prefix.clone());
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_updates_every_prefix() {
        let mut r = Rollup::default();
        r.record("a/b/c", 10);
        let one = Aggregate {
            object_count: 1,
            byte_count: 10,
        };
        assert_eq!(r.get(""), one);
        assert_eq!(r.get("a"), one);
        assert_eq!(r.get("a/b"), one);
        assert_eq!(r.get("a/b/c"), Aggregate::default());
        assert_eq!(r.get("a/b/c/d"), Aggregate::default());
    }

    #[test]
    fn record_accumulates() {
        let mut r = Rollup::default();
        r.record("x/y", 3);
        r.record("x/z", 7);
        assert_eq!(
            r.get("x"),
            Aggregate {
                object_count: 2,
                byte_count: 10,
            }
        );
        assert_eq!(
            r.get(""),
            Aggregate {
                object_count: 2,
                byte_count: 10
            }
        );
    }

    #[test]
    fn deltas_match_record() {
        let deltas = Rollup::deltas_for("x/y/z", 7);
        let prefixes: Vec<&str> = deltas.iter().map(|d| d.prefix.as_str()).collect();
        assert_eq!(prefixes, vec!["", "x", "x/y"]);
        for d in &deltas {
            assert_eq!(d.object_count_delta, 1);
            assert_eq!(d.byte_count_delta, 7);
        }
    }

    #[test]
    fn empty_key_records_root_only() {
        let mut r = Rollup::default();
        r.record("", 3);
        assert_eq!(
            r.get(""),
            Aggregate {
                object_count: 1,
                byte_count: 3,
            }
        );
    }

    #[test]
    fn empty_segments_are_skipped() {
        let mut r = Rollup::default();
        r.record("a//b", 1);
        assert_eq!(r.get("").object_count, 1);
        assert_eq!(r.get("a").object_count, 1);
        assert_eq!(r.get("a/b").object_count, 0);
    }
}
