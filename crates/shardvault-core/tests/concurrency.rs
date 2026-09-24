mod common;

use common::TestDir;
use shardvault_core::segment::Store;
use std::sync::{Arc, Mutex};
use std::thread;

/// 8 reader threads hammer committed snapshots while 1 writer threads puts
/// with a flush (snapshot publish) per round. Every value a reader observes
/// must be one the writer actually wrote; readers must never see torn or
/// half-published state.
#[test]
fn concurrent_readers_never_observe_torn_state() {
    const READERS: usize = 8;
    const ROUNDS: u64 = 200;
    const KEYS: usize = 40;

    let dir = TestDir::new("concurrent");
    let store = Store::open(dir.path()).unwrap();
    // A lock-free read handle, shared by all readers before the writer
    // takes exclusive ownership of the store.
    let view = Arc::new(store.read_view());

    // (key, value) pairs the writer has staged before each flush; a value
    // is observable only after its entry is in this log, so readers can
    // validate every observation.
    let written = Arc::new(Mutex::new(Vec::<(String, Vec<u8>)>::new()));
    let written_for_writer = Arc::clone(&written);

    let mut readers = Vec::new();
    for rid in 0..READERS {
        let view = Arc::clone(&view);
        let written = Arc::clone(&written);
        readers.push(thread::spawn(move || {
            let mut observations = 0usize;
            for _ in 0..400 {
                for i in 0..KEYS {
                    let key = format!("k{i:03}");
                    if let Ok(Some(value)) = view.get(&key) {
                        let history = written
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|(k, _)| *k == key)
                            .map(|(_, v)| v.clone())
                            .collect::<Vec<_>>();
                        assert!(
                            history.contains(&value),
                            "reader {rid} observed a value never written for {key}: {value:?}"
                        );
                        observations += 1;
                    }
                }
                let _ = view.capacity("");
                let _ = view.get_upto("k000", u64::MAX);
            }
            observations
        }));
    }

    let writer = thread::spawn(move || {
        let mut store = store;
        for round in 0..ROUNDS {
            let key = format!("k{:03}", (round as usize) % KEYS);
            let value = vec![(round as u8).wrapping_mul(7); 128];
            store.put(&key, &value).unwrap();
            written_for_writer.lock().unwrap().push((key, value));
            store.flush().unwrap();
        }
    });

    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }

    // Final state: every key present with the last written value.
    let store = Store::open(dir.path()).unwrap();
    for i in 0..KEYS {
        let key = format!("k{i:03}");
        let last = written
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.clone())
            .unwrap();
        assert_eq!(store.get(&key).unwrap().unwrap(), last, "key {key}");
    }
}
