mod common;

use common::TestDir;
use shardvault_core::segment::{Store, StoreOptions};
use std::path::Path;

fn small_opts() -> StoreOptions {
    StoreOptions {
        segment_max_bytes: 2048,
    }
}

fn dir_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn compaction_reclaims_dead_space_and_preserves_live_keys() {
    let dir = TestDir::new("compact-basic");
    let opts = small_opts();
    let mut store = Store::open_with_options(dir.path(), opts).unwrap();

    // Three rounds of overwrites: rounds 0 and 1 become dead values.
    for round in 0..3 {
        for i in 0..40 {
            let key = format!("k{i:03}");
            let value = vec![(i * 7 + round) as u8; 120];
            store.put(&key, &value).unwrap();
        }
        store.flush().unwrap();
    }

    let committed = store.committed_lsn();
    let stage = store
        .compact_stage(committed)
        .unwrap()
        .expect("eligible segment");
    assert!(stage.freed > 0, "compaction must reclaim dead bytes");
    let end_lsn = stage.records.last().map(|(l, _)| *l).unwrap();
    store.commit(end_lsn).unwrap();
    let freed = store.compact_commit(stage).unwrap();
    assert!(freed > 0);

    for i in 0..40 {
        let key = format!("k{i:03}");
        let want = vec![(i * 7 + 2) as u8; 120];
        assert_eq!(store.get(&key).unwrap().unwrap(), want, "key {key}");
    }
    assert_eq!(store.capacity("").object_count, 40);

    drop(store);
    let reopened = Store::open_with_options(dir.path(), opts).unwrap();
    for i in 0..40 {
        let key = format!("k{i:03}");
        let want = vec![(i * 7 + 2) as u8; 120];
        assert_eq!(reopened.get(&key).unwrap().unwrap(), want, "key {key}");
    }
    assert_eq!(reopened.capacity("").object_count, 40);
}

#[test]
fn crash_before_commit_leaves_old_layout() {
    let dir = TestDir::new("compact-crash-before");
    let opts = small_opts();
    {
        let mut store = Store::open_with_options(dir.path(), opts).unwrap();
        for round in 0..3 {
            for i in 0..40 {
                let key = format!("k{i:03}");
                let value = vec![(i * 7 + round) as u8; 120];
                store.put(&key, &value).unwrap();
            }
            store.flush().unwrap();
        }
        let committed = store.committed_lsn();
        let stage = store
            .compact_stage(committed)
            .unwrap()
            .expect("eligible segment");
        drop(stage);
        // Crash: staged but uncommitted compaction, no Drop flush.
        std::mem::forget(store);
    }
    let reopened = Store::open_with_options(dir.path(), opts).unwrap();
    for i in 0..40 {
        let key = format!("k{i:03}");
        let want = vec![(i * 7 + 2) as u8; 120];
        assert_eq!(reopened.get(&key).unwrap().unwrap(), want, "key {key}");
    }
    for name in dir_files(dir.path()) {
        assert!(!name.ends_with(".tmp"), "orphan tmp file {name}");
    }
}

#[test]
fn horizon_protects_recent_dead_values() {
    let dir = TestDir::new("compact-horizon");
    let opts = small_opts();
    let mut store = Store::open_with_options(dir.path(), opts).unwrap();

    store.put("k", &vec![0x11; 400]).unwrap();
    store.flush().unwrap();
    store.put("k", &vec![0x22; 400]).unwrap();
    for i in 0..8 {
        store.put(&format!("f{i}"), &vec![0x33; 400]).unwrap();
    }
    store.flush().unwrap();

    // v1 (the first put, lsn 1) is dead; a horizon of 0 must not reclaim it.
    let none = store.compact_stage(0).unwrap();
    assert!(
        none.is_none(),
        "horizon 0 must not reclaim recent dead values"
    );

    // At the full committed horizon the dead v1 is reclaimed.
    let committed = store.committed_lsn();
    let stage = store
        .compact_stage(committed)
        .unwrap()
        .expect("eligible segment");
    assert!(stage.freed >= 400);
    let end = stage.records.last().map(|(l, _)| *l).unwrap();
    store.commit(end).unwrap();
    store.compact_commit(stage).unwrap();

    assert_eq!(store.get("k").unwrap().unwrap(), vec![0x22; 400]);
    assert_eq!(store.capacity("").object_count, 9);
}

#[test]
fn crash_after_commit_uses_new_layout() {
    let dir = TestDir::new("compact-crash-after");
    let opts = small_opts();
    {
        let mut store = Store::open_with_options(dir.path(), opts).unwrap();
        for round in 0..3 {
            for i in 0..40 {
                let key = format!("k{i:03}");
                let value = vec![(i * 7 + round) as u8; 120];
                store.put(&key, &value).unwrap();
            }
            store.flush().unwrap();
        }
        let committed = store.committed_lsn();
        let stage = store
            .compact_stage(committed)
            .unwrap()
            .expect("eligible segment");
        let end = stage.records.last().map(|(l, _)| *l).unwrap();
        store.commit(end).unwrap();
        store.compact_commit(stage).unwrap();
        // Crash immediately after commit.
        std::mem::forget(store);
    }
    let reopened = Store::open_with_options(dir.path(), opts).unwrap();
    for i in 0..40 {
        let key = format!("k{i:03}");
        let want = vec![(i * 7 + 2) as u8; 120];
        assert_eq!(reopened.get(&key).unwrap().unwrap(), want, "key {key}");
    }
    for name in dir_files(dir.path()) {
        assert!(!name.ends_with(".tmp"), "orphan tmp file {name}");
    }
}

#[test]
fn no_eligible_segment_returns_none() {
    let dir = TestDir::new("compact-none");
    let opts = small_opts();
    let mut store = Store::open_with_options(dir.path(), opts).unwrap();
    for i in 0..5 {
        store.put(&format!("k{i}"), &[i as u8; 100]).unwrap();
    }
    store.flush().unwrap();
    let committed = store.committed_lsn();
    assert!(store.compact_stage(committed).unwrap().is_none());
}
