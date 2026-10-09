mod common;

use common::TestDir;
use shardvault_core::segment::{Store, StoreOptions};
use shardvault_core::wal::Record;
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

/// Leader WAL records after `after`, with value bytes for `Put`s, as a
/// follower would receive them.
fn replication_stream(store: &Store, after: u64) -> Vec<(u64, Record, Option<Vec<u8>>)> {
    store
        .scan_wal()
        .unwrap()
        .into_iter()
        .filter(|(lsn, _)| *lsn > after)
        .map(|(lsn, rec)| {
            let data = match rec {
                Record::Put { .. } => Some(store.value_at(lsn).unwrap()),
                _ => None,
            };
            (lsn, rec, data)
        })
        .collect()
}

#[test]
fn follower_crash_after_swap_sync_before_commit_recovers() {
    let leader_dir = TestDir::new("compact-follower-leader");
    let follower_dir = TestDir::new("compact-follower");
    let opts = small_opts();
    let mut leader = Store::open_with_options(leader_dir.path(), opts).unwrap();
    for round in 0..3 {
        for i in 0..40 {
            let key = format!("k{i:03}");
            let value = vec![(i * 7 + round) as u8; 120];
            leader.put(&key, &value).unwrap();
        }
        leader.flush().unwrap();
    }
    let base = replication_stream(&leader, 0);
    let base_end = base.last().unwrap().0;
    let stage = leader
        .compact_stage(leader.committed_lsn())
        .unwrap()
        .expect("eligible segment");
    let end_lsn = stage.records.last().map(|(l, _)| *l).unwrap();
    leader.commit(end_lsn).unwrap();
    leader.compact_commit(stage).unwrap();
    let compaction = replication_stream(&leader, base_end);
    let old_segment = compaction
        .iter()
        .find_map(|(_, rec, _)| match rec {
            Record::SegmentSwap { old_segment_id, .. } => Some(*old_segment_id),
            _ => None,
        })
        .expect("swap record");
    let old_name = format!("seg-{old_segment:08}.dat");

    let check = |store: &Store| {
        for i in 0..40 {
            let key = format!("k{i:03}");
            let want = vec![(i * 7 + 2) as u8; 120];
            assert_eq!(store.get(&key).unwrap().unwrap(), want, "key {key}");
        }
    };

    {
        let mut follower = Store::open_with_options(follower_dir.path(), opts).unwrap();
        for (lsn, rec, data) in &base {
            follower.apply_record(*lsn, rec, data.as_deref()).unwrap();
        }
        follower.sync_all().unwrap();
        // The leader's sync barrier for the compaction batch arrives before
        // its Commit record: the swap is synced but not yet committed.
        for (lsn, rec, data) in &compaction {
            if !matches!(rec, Record::Commit { .. }) {
                follower.apply_record(*lsn, rec, data.as_deref()).unwrap();
            }
        }
        follower.sync_all().unwrap();
        check(&follower);
        assert!(
            dir_files(follower_dir.path()).contains(&old_name),
            "old segment deleted before its swap was committed"
        );
        // Crash before the Commit record arrives.
        std::mem::forget(follower);
    }

    // Recovery drops the uncommitted swap, so it needs the old segment.
    let mut follower = Store::open_with_options(follower_dir.path(), opts).unwrap();
    check(&follower);

    // The leader re-sends the compaction; once committed and synced, the
    // old segment is gone.
    let from = follower.last_commit_block();
    for (lsn, rec, data) in compaction.iter().filter(|(l, _, _)| *l > from) {
        follower.apply_record(*lsn, rec, data.as_deref()).unwrap();
    }
    follower.sync_all().unwrap();
    check(&follower);
    assert!(!dir_files(follower_dir.path()).contains(&old_name));
    drop(follower);
    let reopened = Store::open_with_options(follower_dir.path(), opts).unwrap();
    check(&reopened);
}

#[test]
fn follower_backfills_consecutive_compactions_without_sync() {
    let leader_dir = TestDir::new("compact-backfill-leader");
    let follower_dir = TestDir::new("compact-backfill");
    let opts = small_opts();
    let mut leader = Store::open_with_options(leader_dir.path(), opts).unwrap();
    for round in 0..4 {
        for i in 0..40 {
            let key = format!("k{i:03}");
            let value = vec![(i * 7 + round) as u8; 120];
            leader.put(&key, &value).unwrap();
        }
        leader.flush().unwrap();
    }
    let base = replication_stream(&leader, 0);
    let base_end = base.last().unwrap().0;
    let mut swaps = 0;
    for _ in 0..3 {
        let Some(stage) = leader.compact_stage(leader.committed_lsn()).unwrap() else {
            break;
        };
        let end_lsn = stage.records.last().map(|(l, _)| *l).unwrap();
        leader.commit(end_lsn).unwrap();
        leader.compact_commit(stage).unwrap();
        swaps += 1;
    }
    assert!(swaps >= 2, "need at least two compactions, got {swaps}");
    let compaction = replication_stream(&leader, base_end);

    let mut follower = Store::open_with_options(follower_dir.path(), opts).unwrap();
    for (lsn, rec, data) in &base {
        follower.apply_record(*lsn, rec, data.as_deref()).unwrap();
    }
    follower.sync_all().unwrap();
    for (lsn, rec, data) in &compaction {
        follower.apply_record(*lsn, rec, data.as_deref()).unwrap();
    }
    follower.sync_all().unwrap();
    drop(follower);

    let reopened = Store::open_with_options(follower_dir.path(), opts).unwrap();
    for i in 0..40 {
        let key = format!("k{i:03}");
        let want = vec![(i * 7 + 3) as u8; 120];
        assert_eq!(reopened.get(&key).unwrap().unwrap(), want, "key {key}");
    }
    assert_eq!(reopened.capacity(""), leader.capacity(""));
}
