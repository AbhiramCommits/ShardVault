//! Write-ahead log.
//!
//! The WAL is a single append-only file (`wal.log`) holding a sequence of
//! 4096-byte blocks produced by the C block layer. Each block carries exactly
//! one [`Record`] in its payload; the block's LSN is the record's LSN.
//!
//! # Commit protocol
//!
//! Durability follows a strict ordering:
//!
//! 1. Data blocks are written to the segment file.
//! 2. The segment file is fsynced (`File::sync_data`).
//! 3. The [`Record::Commit`] record is appended to the WAL.
//! 4. The WAL is fsynced.
//!
//! A `Put` only becomes visible after a `Commit` with `lsn >= put_lsn` is
//! durable. If the crash lands between steps 2 and 4 the commit record never
//! reaches disk, so on recovery the put is discarded and the segment tail it
//! wrote is truncated away.
//!
//! # Recovery
//!
//! [`Wal::replay`] reads blocks sequentially and stops at the first torn
//! block (short read) or CRC failure; it never skips past corruption. Records
//! after the last durable `Commit` are discarded, and the recovered
//! high-water LSN is the maximum of the committed LSN and the last valid
//! block LSN, so LSNs are never reused across recovery.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::Path;

use crate::error::StoreError;
use crate::ffi::block::{self, BlockError};
use crate::rollup::AggDeltaEntry;

pub type Lsn = u64;

pub const WAL_FILE: &str = "wal.log";

const TAG_PUT: u8 = 0;
const TAG_COMMIT: u8 = 1;
const TAG_SEAL: u8 = 2;
const TAG_AGG: u8 = 3;
const TAG_SWAP: u8 = 4;
const TAG_REMAP: u8 = 5;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemapEntry {
    pub key: String,
    pub lsn: u64,
    pub offset: u64,
    pub len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Record {
    Put {
        key: String,
        len: u32,
        stripe_id: u64,
    },
    Commit {
        lsn: u64,
    },
    SegmentSeal {
        segment_id: u64,
        next_segment_id: u64,
    },
    AggDelta {
        deltas: Vec<AggDeltaEntry>,
    },
    SegmentSwap {
        old_segment_id: u64,
        new_segment_id: u64,
    },
    SegmentRemap {
        new_segment_id: u64,
        entries: Vec<RemapEntry>,
    },
}

fn corrupt(msg: &str) -> StoreError {
    StoreError::Corrupt(msg.to_string())
}

fn encode_record(rec: &Record) -> Result<Vec<u8>, StoreError> {
    let mut v = Vec::new();
    match rec {
        Record::Put {
            key,
            len,
            stripe_id,
        } => {
            let key_len = u16::try_from(key.len()).map_err(|_| StoreError::RecordTooLarge)?;
            v.push(TAG_PUT);
            v.extend_from_slice(&stripe_id.to_le_bytes());
            v.extend_from_slice(&len.to_le_bytes());
            v.extend_from_slice(&key_len.to_le_bytes());
            v.extend_from_slice(key.as_bytes());
        }
        Record::Commit { lsn } => {
            v.push(TAG_COMMIT);
            v.extend_from_slice(&lsn.to_le_bytes());
        }
        Record::SegmentSeal {
            segment_id,
            next_segment_id,
        } => {
            v.push(TAG_SEAL);
            v.extend_from_slice(&segment_id.to_le_bytes());
            v.extend_from_slice(&next_segment_id.to_le_bytes());
        }
        Record::AggDelta { deltas } => {
            let n = u16::try_from(deltas.len()).map_err(|_| StoreError::RecordTooLarge)?;
            v.push(TAG_AGG);
            v.extend_from_slice(&n.to_le_bytes());
            for d in deltas {
                let plen = u16::try_from(d.prefix.len()).map_err(|_| StoreError::RecordTooLarge)?;
                v.extend_from_slice(&plen.to_le_bytes());
                v.extend_from_slice(d.prefix.as_bytes());
                v.extend_from_slice(&d.object_count_delta.to_le_bytes());
                v.extend_from_slice(&d.byte_count_delta.to_le_bytes());
            }
        }
        Record::SegmentSwap {
            old_segment_id,
            new_segment_id,
        } => {
            v.push(TAG_SWAP);
            v.extend_from_slice(&old_segment_id.to_le_bytes());
            v.extend_from_slice(&new_segment_id.to_le_bytes());
        }
        Record::SegmentRemap {
            new_segment_id,
            entries,
        } => {
            let n = u16::try_from(entries.len()).map_err(|_| StoreError::RecordTooLarge)?;
            v.push(TAG_REMAP);
            v.extend_from_slice(&new_segment_id.to_le_bytes());
            v.extend_from_slice(&n.to_le_bytes());
            for e in entries {
                let klen = u16::try_from(e.key.len()).map_err(|_| StoreError::RecordTooLarge)?;
                v.extend_from_slice(&klen.to_le_bytes());
                v.extend_from_slice(e.key.as_bytes());
                v.extend_from_slice(&e.lsn.to_le_bytes());
                v.extend_from_slice(&e.offset.to_le_bytes());
                v.extend_from_slice(&e.len.to_le_bytes());
            }
        }
    }
    if v.len() > block::PAYLOAD_MAX {
        return Err(StoreError::RecordTooLarge);
    }
    Ok(v)
}

fn take<'a>(p: &mut &'a [u8], n: usize) -> Result<&'a [u8], StoreError> {
    if p.len() < n {
        return Err(corrupt("truncated record"));
    }
    let (head, tail) = p.split_at(n);
    *p = tail;
    Ok(head)
}

fn take_u64(p: &mut &[u8]) -> Result<u64, StoreError> {
    Ok(u64::from_le_bytes(take(p, 8)?.try_into().unwrap()))
}

fn take_u32(p: &mut &[u8]) -> Result<u32, StoreError> {
    Ok(u32::from_le_bytes(take(p, 4)?.try_into().unwrap()))
}

fn take_u16(p: &mut &[u8]) -> Result<u16, StoreError> {
    Ok(u16::from_le_bytes(take(p, 2)?.try_into().unwrap()))
}

fn take_utf8(p: &mut &[u8], n: usize, what: &str) -> Result<String, StoreError> {
    let bytes = take(p, n)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| corrupt(&format!("invalid utf-8 in {what}")))
}

fn decode_record(payload: &[u8]) -> Result<Record, StoreError> {
    let mut p = payload;
    let tag = take(&mut p, 1)?[0];
    match tag {
        TAG_PUT => {
            let stripe_id = take_u64(&mut p)?;
            let len = take_u32(&mut p)?;
            let klen = take_u16(&mut p)? as usize;
            let key = take_utf8(&mut p, klen, "key")?;
            Ok(Record::Put {
                key,
                len,
                stripe_id,
            })
        }
        TAG_COMMIT => {
            let lsn = take_u64(&mut p)?;
            Ok(Record::Commit { lsn })
        }
        TAG_SEAL => {
            let segment_id = take_u64(&mut p)?;
            let next_segment_id = take_u64(&mut p)?;
            Ok(Record::SegmentSeal {
                segment_id,
                next_segment_id,
            })
        }
        TAG_AGG => {
            let n = take_u16(&mut p)? as usize;
            let mut deltas = Vec::with_capacity(n);
            for _ in 0..n {
                let plen = take_u16(&mut p)? as usize;
                let prefix = take_utf8(&mut p, plen, "prefix")?;
                let object_count_delta = take_u64(&mut p)?;
                let byte_count_delta = take_u64(&mut p)?;
                deltas.push(AggDeltaEntry {
                    prefix,
                    object_count_delta,
                    byte_count_delta,
                });
            }
            Ok(Record::AggDelta { deltas })
        }
        TAG_SWAP => {
            let old_segment_id = take_u64(&mut p)?;
            let new_segment_id = take_u64(&mut p)?;
            Ok(Record::SegmentSwap {
                old_segment_id,
                new_segment_id,
            })
        }
        TAG_REMAP => {
            let new_segment_id = take_u64(&mut p)?;
            let n = take_u16(&mut p)? as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let klen = take_u16(&mut p)? as usize;
                let key = take_utf8(&mut p, klen, "key")?;
                let lsn = take_u64(&mut p)?;
                let offset = take_u64(&mut p)?;
                let len = take_u32(&mut p)?;
                entries.push(RemapEntry {
                    key,
                    lsn,
                    offset,
                    len,
                });
            }
            Ok(Record::SegmentRemap {
                new_segment_id,
                entries,
            })
        }
        other => Err(corrupt(&format!("unknown record tag {other}"))),
    }
}

pub struct Wal {
    file: File,
    buf: Vec<u8>,
    next_lsn: Lsn,
    dirty: bool,
}

impl Wal {
    /// Opens the WAL in `dir`, recovering it: the file is truncated to the
    /// last durable commit and the next LSN continues past the recovered
    /// high-water mark.
    pub fn open(dir: &Path) -> Result<Wal, StoreError> {
        let path = dir.join(WAL_FILE);
        let (records, high_water) = Self::replay(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        file.set_len((records.len() as u64) * block::BLOCK_SIZE as u64)?;
        Ok(Wal {
            file,
            buf: Vec::new(),
            next_lsn: high_water + 1,
            dirty: false,
        })
    }

    /// Appends a record to the in-memory WAL buffer and returns its LSN.
    /// The record is not durable until [`Wal::sync`] is called.
    pub fn append(&mut self, rec: &Record) -> Result<Lsn, StoreError> {
        let lsn = self.next_lsn;
        self.append_at(rec, lsn)?;
        Ok(lsn)
    }

    /// Appends a record at an explicit LSN (used by followers replicating
    /// the leader's record sequence).
    pub fn append_at(&mut self, rec: &Record, lsn: Lsn) -> Result<(), StoreError> {
        let payload = encode_record(rec)?;
        let tag = payload[0];
        let encoded = match block::encode(lsn, &payload, tag) {
            Ok(b) => b,
            Err(BlockError::Len) => return Err(StoreError::RecordTooLarge),
            Err(e) => return Err(StoreError::Corrupt(format!("block encode failed: {e}"))),
        };
        self.buf.extend_from_slice(&encoded);
        self.next_lsn = self.next_lsn.max(lsn + 1);
        self.dirty = true;
        Ok(())
    }

    /// Writes the buffered blocks to the WAL file and fsyncs it.
    pub fn sync(&mut self) -> Result<(), StoreError> {
        if !self.dirty {
            return Ok(());
        }
        self.file.write_all(&self.buf)?;
        crate::fault::maybe_fail();
        self.file.sync_data()?;
        self.buf.clear();
        self.dirty = false;
        Ok(())
    }

    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// Reads the WAL from `path`. Stops at the first torn or corrupt block,
    /// discards records after the last durable `Commit`, and returns the
    /// surviving records plus the recovered high-water LSN.
    pub fn replay(path: &Path) -> Result<(Vec<Record>, Lsn), StoreError> {
        let (records, high_water) = Self::replay_with_lsns(path)?;
        Ok((
            records.into_iter().map(|(_, rec)| rec).collect(),
            high_water,
        ))
    }

    /// Like [`Wal::replay`] but keeps each record's block LSN.
    pub fn replay_with_lsns(path: &Path) -> Result<(Vec<(Lsn, Record)>, Lsn), StoreError> {
        let mut records = Self::scan(path)?;
        let mut high_water = 0u64;
        let mut keep = 0usize;
        for (i, (_, rec)) in records.iter().enumerate().rev() {
            if let Record::Commit { lsn } = rec {
                keep = i + 1;
                high_water = *lsn;
                break;
            }
        }
        let last = records.last().map(|(lsn, _)| *lsn).unwrap_or(0);
        records.truncate(keep);
        high_water = high_water.max(last);
        Ok((records, high_water))
    }

    /// Reads every valid record from the WAL with its block LSN, stopping at
    /// the first torn or corrupt block. No commit filtering is applied.
    pub fn scan(path: &Path) -> Result<Vec<(Lsn, Record)>, StoreError> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut records = Vec::new();
        loop {
            let mut block = [0u8; block::BLOCK_SIZE];
            let mut filled = 0usize;
            while filled < block.len() {
                let n = file.read(&mut block[filled..])?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }
            if filled < block.len() {
                break;
            }
            let decoded = match block::decode(&block) {
                Ok(d) => d,
                Err(_) => break,
            };
            match decode_record(&decoded.payload) {
                Ok(rec) => records.push((decoded.lsn, rec)),
                Err(_) => break,
            }
        }
        Ok(records)
    }
}
