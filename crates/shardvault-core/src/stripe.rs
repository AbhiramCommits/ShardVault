//! Erasure-coded stripes spread over simulated nodes.
//!
//! A stripe of user data is split into k data shards plus m parity shards
//! (Reed-Solomon over GF(2^8)). Each shard lives in its own simulated node
//! directory `nodes/n{0..k+m-1}` as a file of C-encoded, checksummed
//! 4096-byte blocks. The first 8 bytes of shard 0's byte stream carry the
//! original data length, so reads reconstruct the exact object.
//!
//! [`StripeReader::read`] treats any shard whose file is missing or whose
//! block CRC fails as erased and reconstructs it transparently as long as at
//! least k shards survive.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use shardvault_ec::rs::ReedSolomon;

use crate::error::StoreError;
use crate::ffi::block;

const LEN_PREFIX: usize = 8;

fn node_dir(base: &Path, node: usize) -> PathBuf {
    base.join("nodes").join(format!("n{node}"))
}

fn shard_path(base: &Path, node: usize, stripe_id: u64) -> PathBuf {
    node_dir(base, node).join(format!("stripe-{stripe_id}.dat"))
}

fn write_shard(base: &Path, node: usize, stripe_id: u64, shard: &[u8]) -> Result<(), StoreError> {
    let dir = node_dir(base, node);
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("stripe-{stripe_id}.dat"));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    for (i, chunk) in shard.chunks(block::PAYLOAD_MAX).enumerate() {
        let encoded = block::encode(i as u64, chunk, 0)
            .map_err(|e| StoreError::Corrupt(format!("block encode failed: {e}")))?;
        file.write_all(&encoded)?;
    }
    file.sync_data()?;
    Ok(())
}

fn read_shard(base: &Path, node: usize, stripe_id: u64) -> Result<Vec<u8>, StoreError> {
    let path = shard_path(base, node, stripe_id);
    let mut file = File::open(&path)?;
    let mut out = Vec::new();
    let mut blk = [0u8; block::BLOCK_SIZE];
    loop {
        let mut filled = 0usize;
        while filled < blk.len() {
            let n = file.read(&mut blk[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled == 0 {
            break;
        }
        if filled < blk.len() {
            return Err(StoreError::Corrupt(format!("torn block in {path:?}")));
        }
        let decoded = block::decode(&blk)
            .map_err(|e| StoreError::Corrupt(format!("block decode failed in {path:?}: {e}")))?;
        out.extend_from_slice(&decoded.payload);
    }
    if out.is_empty() {
        return Err(StoreError::Corrupt(format!("empty shard file {path:?}")));
    }
    Ok(out)
}

pub struct StripeWriter {
    base: PathBuf,
    rs: ReedSolomon,
}

impl StripeWriter {
    pub fn new(base: &Path, k: usize, m: usize) -> StripeWriter {
        StripeWriter {
            base: base.to_path_buf(),
            rs: ReedSolomon::new(k, m),
        }
    }

    pub fn write(&self, stripe_id: u64, data: &[u8]) -> Result<(), StoreError> {
        let k = self.rs.data_shard_count();
        let shard_len = data.len().div_ceil(k);
        let rs_len = shard_len + LEN_PREFIX;

        let data_shards = (0..k)
            .map(|i| {
                let mut shard = vec![0u8; rs_len];
                if i == 0 {
                    shard[..LEN_PREFIX].copy_from_slice(&(data.len() as u64).to_le_bytes());
                    let copy = data.len().min(shard_len);
                    shard[LEN_PREFIX..LEN_PREFIX + copy].copy_from_slice(&data[..copy]);
                } else {
                    let start = i * shard_len;
                    if start < data.len() {
                        let copy = (data.len() - start).min(shard_len);
                        shard[..copy].copy_from_slice(&data[start..start + copy]);
                    }
                }
                shard
            })
            .collect::<Vec<Vec<u8>>>();
        let parity = self.rs.encode(&data_shards);

        for (node, shard) in data_shards.iter().enumerate() {
            write_shard(&self.base, node, stripe_id, shard)?;
        }
        for (j, shard) in parity.iter().enumerate() {
            write_shard(&self.base, k + j, stripe_id, shard)?;
        }
        Ok(())
    }
}

pub struct StripeReader {
    base: PathBuf,
    rs: ReedSolomon,
}

impl StripeReader {
    pub fn new(base: &Path, k: usize, m: usize) -> StripeReader {
        StripeReader {
            base: base.to_path_buf(),
            rs: ReedSolomon::new(k, m),
        }
    }

    pub fn read(&self, stripe_id: u64) -> Result<Vec<u8>, StoreError> {
        let n = self.rs.shard_count();
        let mut shards: Vec<Option<Vec<u8>>> = (0..n)
            .map(|node| read_shard(&self.base, node, stripe_id))
            .map(|r| r.ok())
            .collect();
        self.rs
            .reconstruct(&mut shards)
            .map_err(|e| StoreError::Corrupt(format!("stripe {stripe_id}: {e}")))?;

        let k = self.rs.data_shard_count();
        let first = shards[0].as_ref().ok_or_else(|| {
            StoreError::Corrupt("data shard 0 missing after reconstruction".into())
        })?;
        if first.len() < LEN_PREFIX {
            return Err(StoreError::Corrupt("shard too short".into()));
        }
        let data_len = u64::from_le_bytes(first[..LEN_PREFIX].try_into().unwrap()) as usize;
        let shard_len = first.len() - LEN_PREFIX;
        if data_len > shard_len * k {
            return Err(StoreError::Corrupt(
                "stripe length field out of range".into(),
            ));
        }
        let mut out = Vec::with_capacity(data_len);
        for (i, s) in shards.iter().take(k).enumerate() {
            let s = s.as_ref().unwrap();
            if s.len() != first.len() {
                return Err(StoreError::Corrupt("shard length mismatch".into()));
            }
            let bytes = if i == 0 {
                &s[LEN_PREFIX..LEN_PREFIX + shard_len]
            } else {
                &s[..shard_len]
            };
            out.extend_from_slice(bytes);
        }
        out.truncate(data_len);
        Ok(out)
    }
}
