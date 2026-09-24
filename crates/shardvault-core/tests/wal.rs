mod common;

use common::TestDir;
use shardvault_core::ffi::block;
use shardvault_core::rollup::AggDeltaEntry;
use shardvault_core::wal::{Record, Wal, WAL_FILE};
use std::fs::OpenOptions;
use std::io::{Read, Write};

fn put_rec(key: &str, len: u32, stripe: u64) -> Record {
    Record::Put {
        key: key.to_string(),
        len,
        stripe_id: stripe,
    }
}

fn agg_rec() -> Record {
    Record::AggDelta {
        deltas: vec![AggDeltaEntry {
            prefix: String::new(),
            object_count_delta: 1,
            byte_count_delta: 5,
        }],
    }
}

fn commit_rec(lsn: u64) -> Record {
    Record::Commit { lsn }
}

#[test]
fn replay_empty_wal() {
    let dir = TestDir::new("wal-empty");
    let (records, high_water) = Wal::replay(&dir.path().join(WAL_FILE)).unwrap();
    assert_eq!(records, Vec::new());
    assert_eq!(high_water, 0);
}

#[test]
fn append_sync_replay_roundtrip() {
    let dir = TestDir::new("wal-roundtrip");
    let wal_path = dir.path().join(WAL_FILE);
    {
        let mut wal = Wal::open(dir.path()).unwrap();
        let l1 = wal.append(&put_rec("a/b", 5, 1)).unwrap();
        let l2 = wal.append(&agg_rec()).unwrap();
        wal.append(&commit_rec(l2)).unwrap();
        wal.sync().unwrap();
        assert_eq!(l1, 1);
        assert_eq!(l2, 2);
    }
    let (records, high_water) = Wal::replay(&wal_path).unwrap();
    assert_eq!(
        records,
        vec![put_rec("a/b", 5, 1), agg_rec(), commit_rec(2)]
    );
    assert_eq!(high_water, 3);
}

#[test]
fn replay_stops_at_torn_block() {
    let dir = TestDir::new("wal-torn");
    let wal_path = dir.path().join(WAL_FILE);
    {
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&put_rec("a", 3, 1)).unwrap();
        wal.append(&commit_rec(1)).unwrap();
        wal.sync().unwrap();
    }
    let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
    f.write_all(&[0xAB; 100]).unwrap();
    drop(f);
    let (records, high_water) = Wal::replay(&wal_path).unwrap();
    assert_eq!(records, vec![put_rec("a", 3, 1), commit_rec(1)]);
    assert_eq!(high_water, 2);
    let _wal = Wal::open(dir.path()).unwrap();
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        2 * block::BLOCK_SIZE as u64
    );
}

#[test]
fn replay_stops_at_crc_failure() {
    let dir = TestDir::new("wal-crc");
    let wal_path = dir.path().join(WAL_FILE);
    {
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&put_rec("a", 3, 1)).unwrap();
        wal.append(&commit_rec(1)).unwrap();
        wal.sync().unwrap();
    }
    let mut blk = [0u8; 4096];
    let mut f = OpenOptions::new().read(true).open(&wal_path).unwrap();
    f.read_exact(&mut blk).unwrap();
    drop(f);
    blk[block::HEADER_SIZE + 2] ^= 0xFF;
    let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
    f.write_all(&blk).unwrap();
    drop(f);
    let (records, high_water) = Wal::replay(&wal_path).unwrap();
    assert_eq!(records, vec![put_rec("a", 3, 1), commit_rec(1)]);
    assert_eq!(high_water, 2);
    let _wal = Wal::open(dir.path()).unwrap();
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        2 * block::BLOCK_SIZE as u64
    );
}

#[test]
fn records_after_last_commit_are_discarded() {
    let dir = TestDir::new("wal-discard");
    let wal_path = dir.path().join(WAL_FILE);
    {
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append(&put_rec("a", 3, 1)).unwrap();
        wal.append(&agg_rec()).unwrap();
        wal.append(&commit_rec(1)).unwrap();
        wal.sync().unwrap();
        wal.append(&put_rec("b", 3, 4)).unwrap();
        wal.sync().unwrap();
    }
    let (records, high_water) = Wal::replay(&wal_path).unwrap();
    assert_eq!(records, vec![put_rec("a", 3, 1), agg_rec(), commit_rec(1)]);
    assert_eq!(high_water, 4);
    let wal = Wal::open(dir.path()).unwrap();
    assert_eq!(wal.next_lsn(), 5);
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        3 * block::BLOCK_SIZE as u64
    );
}
