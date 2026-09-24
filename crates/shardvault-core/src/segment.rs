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

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::StoreError;
use crate::rollup::{Aggregate, Rollup};
use crate::wal::{Record, Wal};

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

pub struct Store {
    dir: PathBuf,
    options: StoreOptions,
    wal: Wal,
    segments: HashMap<u64, File>,
    index: HashMap<String, (u64, u64, u32)>,
    rollup: Rollup,
    active_id: u64,
    active_offset: u64,
    pending: bool,
    dirty_segments: HashSet<u64>,
}

impl Store {
    pub fn open(dir: &Path) -> Result<Store, StoreError> {
        Self::open_with_options(dir, StoreOptions::default())
    }

    pub fn open_with_options(dir: &Path, options: StoreOptions) -> Result<Store, StoreError> {
        fs::create_dir_all(dir)?;
        let (records, _high_water) = Wal::replay(&dir.join(crate::wal::WAL_FILE))?;
        let wal = Wal::open(dir)?;
        let mut store = Store {
            dir: dir.to_path_buf(),
            options,
            wal,
            segments: HashMap::new(),
            index: HashMap::new(),
            rollup: Rollup::default(),
            active_id: 0,
            active_offset: 0,
            pending: false,
            dirty_segments: HashSet::new(),
        };
        store.recover(records)?;
        Ok(store)
    }

    fn recover(&mut self, records: Vec<Record>) -> Result<(), StoreError> {
        let mut committed: HashMap<u64, u64> = HashMap::new();
        for rec in &records {
            match rec {
                Record::Put { key, len, .. } => {
                    let len64 = *len as u64;
                    self.index
                        .insert(key.clone(), (self.active_id, self.active_offset, *len));
                    self.rollup.record(key, len64);
                    self.active_offset += len64;
                    committed.insert(self.active_id, self.active_offset);
                }
                Record::SegmentSeal { segment_id } => {
                    committed.insert(*segment_id, self.active_offset);
                    self.active_id = segment_id + 1;
                    self.active_offset = 0;
                }
                Record::Commit { .. } | Record::AggDelta { .. } => {}
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

    fn seal_active(&mut self) -> Result<(), StoreError> {
        self.wal.append(&Record::SegmentSeal {
            segment_id: self.active_id,
        })?;
        self.pending = true;
        self.active_id += 1;
        self.active_offset = 0;
        Ok(())
    }

    /// Appends `value` under `key`. Returns an [`ObjectId`] (the put's WAL
    /// LSN). The put becomes durable once [`Store::flush`] completes the
    /// commit protocol (segment fsync, then WAL fsync).
    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<ObjectId, StoreError> {
        if value.len() > u32::MAX as usize {
            return Err(StoreError::RecordTooLarge);
        }
        let len = value.len() as u64;
        if self.active_offset > 0 && self.active_offset + len > self.options.segment_max_bytes {
            self.seal_active()?;
        }
        self.open_segment(self.active_id)?;
        let file = self.segments.get(&self.active_id).unwrap();
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
        let stripe_id = self.wal.next_lsn();
        self.wal.append(&Record::Put {
            key: key.to_string(),
            len: value.len() as u32,
            stripe_id,
        })?;
        self.wal.append(&Record::AggDelta {
            deltas: Rollup::deltas_for(key, len),
        })?;
        self.index.insert(
            key.to_string(),
            (self.active_id, self.active_offset, value.len() as u32),
        );
        self.rollup.record(key, len);
        self.active_offset += len;
        self.dirty_segments.insert(self.active_id);
        self.pending = true;
        if len > self.options.segment_max_bytes {
            self.seal_active()?;
        }
        Ok(stripe_id)
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(&(seg_id, offset, len)) = self.index.get(key) else {
            return Ok(None);
        };
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
        Ok(Some(buf))
    }

    /// Performs the commit protocol: fsyncs dirty segment files, then
    /// appends a `Commit` record covering all pending puts and fsyncs the
    /// WAL. Everything appended since the last flush becomes durable.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        if !self.pending {
            return Ok(());
        }
        for id in &self.dirty_segments {
            if let Some(file) = self.segments.get(id) {
                file.sync_data()?;
            }
        }
        let commit_lsn = self.wal.next_lsn() - 1;
        self.wal.append(&Record::Commit { lsn: commit_lsn })?;
        self.wal.sync()?;
        self.pending = false;
        self.dirty_segments.clear();
        Ok(())
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
