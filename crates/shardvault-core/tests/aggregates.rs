mod common;

use common::TestDir;
use shardvault_core::rollup::Aggregate;
use shardvault_core::segment::Store;
use std::collections::HashMap;
use std::time::{Duration, Instant};

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

#[test]
fn aggregates_match_bruteforce_after_100k_puts() {
    const N: u64 = 100_000;
    let dir = TestDir::new("agg-100k");
    let mut store = Store::open(dir.path()).unwrap();
    let mut brute: HashMap<String, Aggregate> = HashMap::new();
    for i in 0..N {
        let key = format!("ns{:02}/s{:02}/k{i:06}", i % 40, i % 25);
        let value = vec![(i & 0xFF) as u8; 16];
        let len = value.len() as u64;
        for prefix in prefixes_of(&key) {
            let agg = brute.entry(prefix).or_default();
            agg.object_count += 1;
            agg.byte_count += len;
        }
        store.put(&key, &value).unwrap();
        if i % 10_000 == 9_999 {
            store.flush().unwrap();
        }
    }
    store.flush().unwrap();

    let expected = brute[&String::new()];
    let start = Instant::now();
    let got = store.capacity("");
    let elapsed = start.elapsed();
    assert_eq!(got, expected);
    assert!(
        elapsed < Duration::from_millis(1),
        "capacity(\"\") took {elapsed:?}"
    );
    assert_eq!(expected.object_count, N);
    assert_eq!(expected.byte_count, N * 16);

    for prefix in ["ns03/s07", "ns39/s24", "ns00/s00", "no/such/prefix"] {
        let got = store.capacity(prefix);
        let exp = brute.get(prefix).copied().unwrap_or_default();
        assert_eq!(got, exp, "prefix {prefix}");
    }
}
