use shardvault_ec::rs::{EcError, ReedSolomon};

fn make_data_shards(k: usize, shard_len: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..shard_len)
                .map(|p| ((i * 31 + p * 7 + 11) & 0xFF) as u8)
                .collect()
        })
        .collect()
}

#[test]
fn reconstruction_all_erasure_combinations() {
    for &(k, m) in &[(4usize, 2usize), (6, 3), (10, 4)] {
        let rs = ReedSolomon::new(k, m);
        let data = make_data_shards(k, 512);
        let parity = rs.encode(&data);
        let mut all = Vec::with_capacity(k + m);
        all.extend(data.clone());
        all.extend(parity);
        let n = k + m;
        for mask in 0u32..(1u32 << n) {
            let erased = mask.count_ones() as usize;
            if erased > m + 1 {
                continue;
            }
            let mut shards: Vec<Option<Vec<u8>>> = all.iter().cloned().map(Some).collect();
            for (i, slot) in shards.iter_mut().enumerate() {
                if mask & (1 << i) != 0 {
                    *slot = None;
                }
            }
            let res = rs.reconstruct(&mut shards);
            if erased <= m {
                res.unwrap();
                for (rebuilt, original) in shards.iter().zip(data.iter()) {
                    assert_eq!(
                        rebuilt.as_ref().unwrap(),
                        original,
                        "k={k} m={m} mask={mask:#b}"
                    );
                }
            } else {
                assert_eq!(
                    res,
                    Err(EcError::TooFewShards),
                    "k={k} m={m} mask={mask:#b}"
                );
            }
        }
    }
}

#[test]
fn no_erasures_is_noop() {
    let rs = ReedSolomon::new(4, 2);
    let data = make_data_shards(4, 64);
    let parity = rs.encode(&data);
    let mut shards: Vec<Option<Vec<u8>>> = data
        .iter()
        .chain(parity.iter())
        .cloned()
        .map(Some)
        .collect();
    let before = shards.clone();
    rs.reconstruct(&mut shards).unwrap();
    assert_eq!(shards, before);
}

#[test]
fn inconsistent_lengths_are_rejected() {
    let rs = ReedSolomon::new(4, 2);
    let data = make_data_shards(4, 64);
    let parity = rs.encode(&data);
    let mut shards: Vec<Option<Vec<u8>>> = vec![
        Some(vec![0; 64]),
        Some(vec![0; 63]),
        Some(vec![0; 64]),
        Some(vec![0; 64]),
        Some(parity[0].clone()),
        Some(parity[1].clone()),
    ];
    assert_eq!(
        rs.reconstruct(&mut shards),
        Err(EcError::InconsistentShards)
    );
}

#[test]
fn wrong_shard_count_is_rejected() {
    let rs = ReedSolomon::new(4, 2);
    let mut shards: Vec<Option<Vec<u8>>> = vec![Some(vec![0; 8]); 5];
    assert_eq!(
        rs.reconstruct(&mut shards),
        Err(EcError::InconsistentShards)
    );
}

#[test]
fn parity_shards_match_manual_computation() {
    let rs = ReedSolomon::new(2, 1);
    let data = vec![vec![0x11, 0x22], vec![0x33, 0x44]];
    let parity = rs.encode(&data);
    assert_eq!(parity[0], vec![0x2D, 0x66]);
}
