mod common;

use common::TestDir;
use shardvault_core::stripe::{StripeReader, StripeWriter};
use std::path::Path;

fn corrupt_shard(dir: &Path, node: usize, stripe_id: u64) {
    let path = dir.join(format!("nodes/n{node}/stripe-{stripe_id}.dat"));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[24 + 5] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
}

fn delete_shard(dir: &Path, node: usize, stripe_id: u64) {
    let path = dir.join(format!("nodes/n{node}/stripe-{stripe_id}.dat"));
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn stripe_roundtrip_various_sizes() {
    let dir = TestDir::new("stripe-roundtrip");
    let writer = StripeWriter::new(dir.path(), 4, 2);
    let reader = StripeReader::new(dir.path(), 4, 2);
    for &size in &[0usize, 1, 7, 8, 10007, 65536] {
        let data: Vec<u8> = (0..size).map(|i| ((i * 37 + 5) & 0xFF) as u8).collect();
        writer.write(size as u64, &data).unwrap();
        assert_eq!(reader.read(size as u64).unwrap(), data, "size {size}");
    }
}

#[test]
fn two_erasures_recover_with_4_2() {
    let dir = TestDir::new("stripe-recover-2");
    let data: Vec<u8> = (0..10007).map(|i| (i & 0xFF) as u8).collect();
    StripeWriter::new(dir.path(), 4, 2).write(1, &data).unwrap();
    delete_shard(dir.path(), 0, 1);
    delete_shard(dir.path(), 3, 1);
    let reader = StripeReader::new(dir.path(), 4, 2);
    assert_eq!(reader.read(1).unwrap(), data);
}

#[test]
fn three_erasures_fail_with_4_2() {
    let dir = TestDir::new("stripe-fail-3");
    let data: Vec<u8> = (0..10007).map(|i| (i & 0xFF) as u8).collect();
    StripeWriter::new(dir.path(), 4, 2).write(1, &data).unwrap();
    delete_shard(dir.path(), 0, 1);
    delete_shard(dir.path(), 3, 1);
    corrupt_shard(dir.path(), 5, 1);
    let reader = StripeReader::new(dir.path(), 4, 2);
    assert!(reader.read(1).is_err());
}

#[test]
fn three_erasures_recover_with_4_3() {
    let dir = TestDir::new("stripe-recover-3");
    let data: Vec<u8> = (0..10007).map(|i| (i & 0xFF) as u8).collect();
    StripeWriter::new(dir.path(), 4, 3).write(1, &data).unwrap();
    delete_shard(dir.path(), 0, 1);
    delete_shard(dir.path(), 3, 1);
    corrupt_shard(dir.path(), 6, 1);
    let reader = StripeReader::new(dir.path(), 4, 3);
    assert_eq!(reader.read(1).unwrap(), data);
}

#[test]
fn zero_length_stripe_roundtrip() {
    let dir = TestDir::new("stripe-empty");
    StripeWriter::new(dir.path(), 4, 2).write(9, &[]).unwrap();
    assert_eq!(
        StripeReader::new(dir.path(), 4, 2).read(9).unwrap(),
        Vec::new()
    );
}
