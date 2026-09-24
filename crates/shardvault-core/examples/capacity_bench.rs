use shardvault_core::segment::Store;
use std::time::Instant;

fn main() {
    let dir = std::env::temp_dir().join(format!("sv-capacity-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut store = Store::open(&dir).unwrap();

    const OBJECTS: u64 = 100_000;
    for i in 0..OBJECTS {
        let key = format!("ns{:02}/s{:02}/k{i:06}", i % 40, i % 25);
        let value = vec![(i & 0xFF) as u8; 16];
        store.put(&key, &value).unwrap();
        if i % 10_000 == 9_999 {
            store.flush().unwrap();
        }
    }
    store.flush().unwrap();

    const LOOKUPS: u64 = 1_000_000;
    for _ in 0..1000 {
        let _ = store.capacity("");
    }
    let start = Instant::now();
    let mut acc = 0u64;
    for _ in 0..LOOKUPS {
        acc += store.capacity("").object_count;
    }
    let elapsed = start.elapsed();
    let per = elapsed.as_nanos() as f64 / LOOKUPS as f64;
    println!(
        "objects={OBJECTS} aggregate_count={acc} lookups={LOOKUPS} elapsed={elapsed:?} per_lookup={per:.1} ns"
    );
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}
