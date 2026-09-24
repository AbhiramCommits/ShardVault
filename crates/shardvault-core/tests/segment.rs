mod common;

use common::TestDir;
use shardvault_core::segment::{Store, StoreOptions};

#[test]
fn put_get_roundtrip() {
    let dir = TestDir::new("seg-roundtrip");
    let mut store = Store::open(dir.path()).unwrap();
    let id1 = store.put("alpha", b"hello").unwrap();
    let id2 = store.put("beta", b"world!").unwrap();
    let id3 = store.put("alpha", b"overwritten").unwrap();
    store.put("empty", b"").unwrap();
    store.flush().unwrap();
    assert!(id1 < id2 && id2 < id3);
    assert_eq!(store.get("alpha").unwrap(), Some(b"overwritten".to_vec()));
    assert_eq!(store.get("beta").unwrap(), Some(b"world!".to_vec()));
    assert_eq!(store.get("gamma").unwrap(), None);
    assert_eq!(store.get("empty").unwrap(), Some(Vec::new()));
}

#[test]
fn reopen_preserves_keys() {
    let dir = TestDir::new("seg-reopen");
    let keys: Vec<(String, Vec<u8>)> = (0..50)
        .map(|i| (format!("ns/obj-{i:03}"), vec![i as u8; 64]))
        .collect();
    {
        let mut store = Store::open(dir.path()).unwrap();
        for (k, v) in &keys {
            store.put(k, v).unwrap();
        }
        store.flush().unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    for (k, v) in &keys {
        assert_eq!(store.get(k).unwrap().as_deref(), Some(v.as_slice()));
    }
    assert_eq!(store.get("missing").unwrap(), None);
    assert_eq!(
        store.capacity(""),
        shardvault_core::rollup::Aggregate {
            object_count: keys.len() as u64,
            byte_count: keys.iter().map(|(_, v)| v.len() as u64).sum(),
        }
    );
}

#[test]
fn uncommitted_tail_is_discarded_after_crash() {
    let dir = TestDir::new("seg-crash");
    let seg0 = dir.path().join("seg-00000000.dat");
    {
        let mut store = Store::open(dir.path()).unwrap();
        store.put("k1", &[0x11; 100]).unwrap();
        store.flush().unwrap();
        store.put("k2", &[0x22; 100]).unwrap();
        store.flush().unwrap();
        store.put("k3", &[0x33; 100]).unwrap();
        std::mem::forget(store);
    }
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.get("k1").unwrap(), Some(vec![0x11; 100]));
    assert_eq!(store.get("k2").unwrap(), Some(vec![0x22; 100]));
    assert_eq!(store.get("k3").unwrap(), None);
    assert_eq!(std::fs::metadata(&seg0).unwrap().len(), 200);
}

#[test]
fn sealed_segment_rollover() {
    let dir = TestDir::new("seg-rollover");
    let opts = StoreOptions {
        segment_max_bytes: 1024,
    };
    {
        let mut store = Store::open_with_options(dir.path(), opts).unwrap();
        for i in 0..12 {
            store
                .put(&format!("key-{i:02}"), &vec![i as u8; 300])
                .unwrap();
        }
        store.flush().unwrap();
        for i in 0..12 {
            assert_eq!(
                store.get(&format!("key-{i:02}")).unwrap(),
                Some(vec![i as u8; 300])
            );
        }
        assert_eq!(store.capacity("").object_count, 12);
    }
    for id in 0..4 {
        let path = dir.path().join(format!("seg-{id:08}.dat"));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 900);
    }
    let store = Store::open_with_options(dir.path(), opts).unwrap();
    for i in 0..12 {
        assert_eq!(
            store.get(&format!("key-{i:02}")).unwrap(),
            Some(vec![i as u8; 300])
        );
    }
}

#[test]
fn oversized_single_object_gets_own_segment() {
    let dir = TestDir::new("seg-oversize");
    let opts = StoreOptions {
        segment_max_bytes: 1024,
    };
    {
        let mut store = Store::open_with_options(dir.path(), opts).unwrap();
        store.put("big", &[0xEE; 5000]).unwrap();
        store.put("small", &[0x01; 16]).unwrap();
        store.flush().unwrap();
        assert_eq!(store.get("big").unwrap(), Some(vec![0xEE; 5000]));
        assert_eq!(store.get("small").unwrap(), Some(vec![0x01; 16]));
    }
    assert_eq!(
        std::fs::metadata(dir.path().join("seg-00000000.dat"))
            .unwrap()
            .len(),
        5000
    );
    let store = Store::open_with_options(dir.path(), opts).unwrap();
    assert_eq!(store.get("big").unwrap(), Some(vec![0xEE; 5000]));
    assert_eq!(store.get("small").unwrap(), Some(vec![0x01; 16]));
}
