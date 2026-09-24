//! Append-only segment store.
//!
//! Objects live in segment files `seg-%08d.dat`. Each `put` appends the raw
//! value bytes to the active segment, records a `Put` plus an `AggDelta` in
//! the WAL, and updates the in-memory index. A segment is sealed once the
//! next put would exceed `segment_max_bytes`; the seal is a WAL record so
//! recovery reproduces the exact segment layout.
//!
//! # Recovery
//!
//! `Store::open` replays the WAL (committed records only), rebuilds the
//! key index and the prefix aggregates, truncates every segment file to its
//! last committed length, and resumes appending past the recovered
//! high-water LSN.
//!
//! # Replication hooks
//!
//! The store supports single-leader replication: [`Store::stage_put`]
//! appends records without committing, [`Store::commit`] appends the
//! `Commit` record once a quorum is reached, and [`Store::apply_record`]
//! replays leader records on a follower at explicit LSNs so follower
//! segment layouts and LSN sequences match the leader's exactly.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::StoreError;
use crate::rollup::{Aggregate, Rollup};
use crate::wal::{Lsn, Record, Wal};

pub type ObjectId = u64;

#[derive(Debug, Clone, Copy)]
pub struct StoreOptions {
    pub segment_max_bytes: u64,
}

impl Default for StoreOptions {
    fn default() -> Self {
        StoreOptions {
            segment_max_bytes: 16 * 1024 * 1024,
        }
    }
}

const SEG_PREFIX: &str = "seg-";
const SEG_SUFFIX: &str = ".dat";

fn seg_name(id: u64) -> String {
    format!("{SEG_PREFIX}{id:08}{SEG_SUFFIX}")
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(SEG_PREFIX)?;
    let rest = rest.strip_suffix(SEG_SUFFIX)?;
    if rest.len() != 8 || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

pub struct StagedPut {
    pub object_id: ObjectId,
    pub records: Vec<(Lsn, Record)>,
}

pub struct Store {
    dir: PathBuf,
    options: StoreOptions,
    wal: Wal,
    segments: HashMap<u64, File>,
    index: HashMap<String, (u64, u64, u32, Lsn)>,
    put_locations: HashMap<Lsn, (u64, u64, u32)>,
    rollup: Rollup,
    active_id: u64,
    active_offset: u64,
    pending: bool,
    committed_lsn: Lsn,
    last_commit_block: Lsn,
    applied_upto: Lsn,
    dirty_segments: HashSet<u64>,
}

impl Store {
    pub fn open(dir: &Path) -> Result<Store, StoreError> {
        Self::open_with_options(dir, StoreOptions::default())
    }

    pub fn open_with_options(dir: &Path, options: StoreOptions) -> Result<Store, StoreError> {
        fs::create_dir_all(dir)?;
        let (records, _high_water) = Wal::replay_with_lsns(&dir.join(crate::wal::WAL_FILE))?;
        let wal = Wal::open(dir)?;
        let mut store = Store {
            dir: dir.to_path_buf(),
            options,
            wal,
            segments: HashMap::new(),
            index: HashMap::new(),
            put_locations: HashMap::new(),
            rollup: Rollup::default(),
            active_id: 0,
            active_offset: 0,
            pending: false,
            committed_lsn: 0,
            last_commit_block: 0,
            applied_upto: 0,
            dirty_segments: HashSet::new(),
        };
        store.recover(records)?;
        Ok(store)
    }

    fn recover(&mut self, records: Vec<(Lsn, Record)>) -> Result<(), StoreError> {
        let mut committed: HashMap<u64, u64> = HashMap::new();
        for (lsn, rec) in &records {
            self.applied_upto = self.applied_upto.max(*lsn);
            match rec {
                Record::Put { key, len, .. } => {
                    let len64 = *len as u64;
                    let loc = (self.active_id, self.active_offset, *len);
                    self.index.insert(key.clone(), (loc.0, loc.1, loc.2, *lsn));
                    self.put_locations.insert(*lsn, loc);
                    self.rollup.record(key, len64);
                    self.active_offset += len64;
                    committed.insert(self.active_id, self.active_offset);
                }
                Record::SegmentSeal { segment_id } => {
                    committed.insert(*segment_id, self.active_offset);
                    self.active_id = segment_id + 1;
                    self.active_offset = 0;
                }
                Record::Commit { lsn: c } => {
                    self.committed_lsn = self.committed_lsn.max(*c);
                    self.last_commit_block = self.last_commit_block.max(*lsn);
                }
                Record::AggDelta { .. } => {}
            }
        }
        committed
            .entry(self.active_id)
            .or_insert(self.active_offset);

        let mut existing: HashMap<u64, PathBuf> = HashMap::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy().into_owned();
            if let Some(id) = parse_segment_name(&name) {
                existing.insert(id, entry.path());
            }
        }
        for (id, target) in &committed {
            if *target > 0 && !existing.contains_key(id) {
                return Err(StoreError::Corrupt(format!(
                    "segment {} referenced by WAL is missing",
                    seg_name(*id)
                )));
            }
        }
        for (id, path) in &existing {
            let target = committed.get(id).copied().unwrap_or(0);
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            let actual = file.metadata()?.len();
            if actual < target {
                return Err(StoreError::Corrupt(format!(
                    "segment {} is shorter than its committed length",
                    seg_name(*id)
                )));
            }
            file.set_len(target)?;
        }
        for id in committed.keys() {
            self.open_segment(*id)?;
        }
        Ok(())
    }

    fn open_segment(&mut self, id: u64) -> Result<(), StoreError> {
        if self.segments.contains_key(&id) {
            return Ok(());
        }
        let path = self.dir.join(seg_name(id));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        self.segments.insert(id, file);
        Ok(())
    }

    fn seal_active_record(&mut self) -> Result<(Lsn, Record), StoreError> {
        let lsn = self.wal.next_lsn();
        let rec = Record::SegmentSeal {
            segment_id: self.active_id,
        };
        self.wal.append(&rec)?;
        self.pending = true;
        self.active_id += 1;
        self.active_offset = 0;
        Ok((lsn, rec))
    }

    fn write_value(&self, value: &[u8]) -> Result<(), StoreError> {
        let file = self
            .segments
            .get(&self.active_id)
            .ok_or_else(|| StoreError::Corrupt("active segment not open".into()))?;
        let mut written = 0usize;
        while written < value.len() {
            let n = file.write_at(&value[written..], self.active_offset + written as u64)?;
            if n == 0 {
                return Err(StoreError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short segment write",
                )));
            }
            written += n;
        }
        Ok(())
    }

    /// Appends `value` under `key` without committing: the `Put` (plus any
    /// `SegmentSeal` and `AggDelta`) records are appended to the WAL buffer
    /// and returned. Durability is the caller's responsibility via
    /// [`Store::sync_wal`] and [`Store::commit`].
    pub fn stage_put(&mut self, key: &str, value: &[u8]) -> Result<StagedPut, StoreError> {
        if value.len() > u32::MAX as usize {
            return Err(StoreError::RecordTooLarge);
        }
        let len = value.len() as u64;
        let mut records = Vec::new();
        if self.active_offset > 0 && self.active_offset + len > self.options.segment_max_bytes {
            records.push(self.seal_active_record()?);
        }
        self.open_segment(self.active_id)?;
        self.write_value(value)?;

        let put_lsn = self.wal.next_lsn();
        let put_rec = Record::Put {
            key: key.to_string(),
            len: value.len() as u32,
            stripe_id: put_lsn,
        };
        self.wal.append(&put_rec)?;
        records.push((put_lsn, put_rec));

        let agg_lsn = self.wal.next_lsn();
        let deltas = Rollup::deltas_for(key, len);
        let agg_rec = Record::AggDelta { deltas };
        self.wal.append(&agg_rec)?;
        records.push((agg_lsn, agg_rec));

        let loc = (self.active_id, self.active_offset, value.len() as u32);
        self.index
            .insert(key.to_string(), (loc.0, loc.1, loc.2, put_lsn));
        self.put_locations.insert(put_lsn, loc);
        self.rollup.record(key, len);
        self.active_offset += len;
        self.dirty_segments.insert(self.active_id);
        self.pending = true;
        if len > self.options.segment_max_bytes {
            records.push(self.seal_active_record()?);
        }
        Ok(StagedPut {
            object_id: put_lsn,
            records,
        })
    }

    /// Appends `value` under `key` without committing. Returns the put's
    /// [`ObjectId`]. The put is durable once [`Store::flush`] (or
    /// [`Store::commit`]) completes the commit protocol.
    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<ObjectId, StoreError> {
        self.stage_put(key, value).map(|staged| staged.object_id)
    }

    /// Applies a replicated leader record at an explicit LSN. Used by
    /// followers; segment layout decisions are driven entirely by the
    /// replicated `SegmentSeal` records, so follower and leader layouts
    /// match exactly.
    pub fn apply_record(
        &mut self,
        lsn: Lsn,
        rec: &Record,
        data: Option<&[u8]>,
    ) -> Result<(), StoreError> {
        self.applied_upto = self.applied_upto.max(lsn);
        match rec {
            Record::Put { key, len, .. } => {
                let data = data.ok_or_else(|| {
                    StoreError::Corrupt("Put record without value data".to_string())
                })?;
                if data.len() != *len as usize {
                    return Err(StoreError::Corrupt("Put value length mismatch".to_string()));
                }
                self.open_segment(self.active_id)?;
                self.write_value(data)?;
                self.wal.append_at(rec, lsn)?;
                let loc = (self.active_id, self.active_offset, *len);
                self.index.insert(key.clone(), (loc.0, loc.1, loc.2, lsn));
                self.put_locations.insert(lsn, loc);
                self.rollup.record(key, data.len() as u64);
                self.active_offset += data.len() as u64;
                self.dirty_segments.insert(self.active_id);
                self.pending = true;
            }
            Record::SegmentSeal { segment_id } => {
                if *segment_id != self.active_id {
                    return Err(StoreError::Corrupt(format!(
                        "seal for segment {} but follower active segment is {}",
                        segment_id, self.active_id
                    )));
                }
                self.wal.append_at(rec, lsn)?;
                self.active_id += 1;
                self.active_offset = 0;
                self.pending = true;
            }
            Record::AggDelta { .. } => {
                self.wal.append_at(rec, lsn)?;
                self.pending = true;
            }
            Record::Commit { lsn: committed } => {
                self.wal.append_at(rec, lsn)?;
                self.committed_lsn = self.committed_lsn.max(*committed);
                self.last_commit_block = self.last_commit_block.max(lsn);
                self.pending = true;
            }
        }
        Ok(())
    }

    /// Fsyncs the WAL buffer without appending a commit record.
    pub fn sync_wal(&mut self) -> Result<(), StoreError> {
        self.wal.sync()
    }

    fn fsync_dirty_segments(&mut self) -> Result<(), StoreError> {
        let ids: Vec<u64> = self.dirty_segments.iter().copied().collect();
        for id in ids {
            crate::fault::maybe_fail();
            if let Some(file) = self.segments.get(&id) {
                file.sync_data()?;
            }
        }
        Ok(())
    }

    /// Fsyncs dirty segment files and the WAL (follower durability point).
    pub fn sync_all(&mut self) -> Result<(), StoreError> {
        self.fsync_dirty_segments()?;
        self.wal.sync()?;
        self.dirty_segments.clear();
        Ok(())
    }

    /// Commit protocol: fsync dirty segments, append `Commit { lsn }`, and
    /// fsync the WAL. All records with LSN <= `lsn` become durable.
    pub fn commit(&mut self, lsn: Lsn) -> Result<(), StoreError> {
        if !self.pending {
            return Ok(());
        }
        self.fsync_dirty_segments()?;
        self.wal.append(&Record::Commit { lsn })?;
        self.wal.sync()?;
        let commit_block = self.wal.next_lsn() - 1;
        self.committed_lsn = self.committed_lsn.max(lsn);
        self.last_commit_block = self.last_commit_block.max(commit_block);
        self.applied_upto = self.applied_upto.max(commit_block);
        self.pending = false;
        self.dirty_segments.clear();
        Ok(())
    }

    /// Commits everything currently staged.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        if !self.pending {
            return Ok(());
        }
        let commit_lsn = self.wal.next_lsn() - 1;
        self.commit(commit_lsn)
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(&(seg_id, offset, len, _lsn)) = self.index.get(key) else {
            return Ok(None);
        };
        self.read_value(seg_id, offset, len).map(Some)
    }

    /// Like [`Store::get`], but only returns values whose put LSN is at or
    /// below `committed`. This is the read-your-writes gate used by the
    /// leader to serve reads only up to its commit index.
    pub fn get_upto(&self, key: &str, committed: Lsn) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(&(seg_id, offset, len, lsn)) = self.index.get(key) else {
            return Ok(None);
        };
        if lsn > committed {
            return Ok(None);
        }
        self.read_value(seg_id, offset, len).map(Some)
    }

    fn read_value(&self, seg_id: u64, offset: u64, len: u32) -> Result<Vec<u8>, StoreError> {
        let file = self
            .segments
            .get(&seg_id)
            .ok_or_else(|| StoreError::Corrupt(format!("segment {} not open", seg_name(seg_id))))?;
        let mut buf = vec![0u8; len as usize];
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = file.read_at(&mut buf[filled..], offset + filled as u64)?;
            if n == 0 {
                return Err(StoreError::Corrupt("short segment read".to_string()));
            }
            filled += n;
        }
        Ok(buf)
    }

    /// Reads the value written by the put at `lsn` (used for backfill).
    pub fn value_at(&self, lsn: Lsn) -> Result<Vec<u8>, StoreError> {
        let &(seg_id, offset, len) = self
            .put_locations
            .get(&lsn)
            .ok_or_else(|| StoreError::Corrupt(format!("no put at lsn {lsn}")))?;
        self.read_value(seg_id, offset, len)
    }

    /// Reads the leader's WAL records (for backfill).
    pub fn scan_wal(&self) -> Result<Vec<(Lsn, Record)>, StoreError> {
        Wal::scan(&self.dir.join(crate::wal::WAL_FILE))
    }

    pub fn next_lsn(&self) -> Lsn {
        self.wal.next_lsn()
    }

    pub fn committed_lsn(&self) -> Lsn {
        self.committed_lsn
    }

    /// Block LSN of the most recent durable `Commit` record. A follower
    /// asks the leader for records strictly above this.
    pub fn last_commit_block(&self) -> Lsn {
        self.last_commit_block
    }

    /// Highest record LSN that has been applied (durable or not).
    pub fn applied_upto(&self) -> Lsn {
        self.applied_upto
    }

    pub fn keys(&self) -> Vec<String> {
        self.index.keys().cloned().collect()
    }

    pub fn committed_keys(&self) -> Vec<(String, u32)> {
        self.index
            .iter()
            .filter(|(_, (_, _, _, lsn))| *lsn <= self.committed_lsn)
            .map(|(k, (_, _, len, _))| (k.clone(), *len))
            .collect()
    }

    pub fn capacity(&self, prefix: &str) -> Aggregate {
        self.rollup.get(prefix)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort durability on clean shutdown; callers that need to know
        // the outcome must call `flush` explicitly.
        let _ = self.flush();
    }
}
