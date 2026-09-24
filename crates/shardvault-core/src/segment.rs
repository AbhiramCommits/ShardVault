//! Append-only segment store with lock-free reads.
//!
//! Objects live in segment files `seg-%08d.dat`. Each `put` appends the raw
//! value bytes to the active segment, records a `Put` plus an `AggDelta` in
//! the WAL, and updates the in-memory index. A segment is sealed once the
//! next put would exceed `segment_max_bytes`; the seal is a WAL record so
//! recovery reproduces the exact segment layout.
//!
//! # Concurrency
//!
//! The store has one writer (all mutating methods take `&mut self` and hold
//! an internal [`RwLock`]) and any number of lock-free readers. Readers
//! load an [`arc_swap::ArcSwap`] snapshot containing the committed index,
//! the open segment file handles, and the aggregates; the writer publishes
//! a new snapshot at each commit. Readers therefore never take a lock and
//! never block on the write path — they observe fully committed states.
//! `get` serves the committed snapshot only, which is exactly the
//! durability contract.
//!
//! # Recovery
//!
//! `Store::open` replays the WAL (committed records only), rebuilds the
//! key index and the prefix aggregates, truncates every segment file to its
//! last committed length, and resumes appending past the recovered
//! high-water LSN.
//!
//! # Compaction
//!
//! [`Store::compact_stage`] rewrites a sealed segment keeping only the
//! values that may still be read: live versions plus dead versions newer
//! than a caller-supplied horizon (a lagging follower may still request
//! those). The new file is written to a temporary name, fsynced, and
//! renamed into place; a `SegmentSwap` record plus chunked `SegmentRemap`
//! records describe the new layout in the WAL. The caller commits the
//! records through the normal commit protocol and then calls
//! [`Store::compact_commit`]. A crash before the commit leaves the old
//! segment fully valid (the new file is orphaned and truncated away);
//! a crash after it finds the new file fully written and fsynced.
//!
//! # Replication hooks
//!
//! The store supports single-leader replication: [`Store::stage_put`]
//! appends records without committing, [`Store::commit`] appends the
//! `Commit` record once a quorum is reached, and [`Store::apply_record`]
//! replays leader records on a follower at explicit LSNs so follower
//! segment layouts and LSN sequences match the leader's exactly.
//! `SegmentSwap`/`SegmentRemap` records are applied by followers by
//! rebuilding the replacement segment from their own copies of the
//! values, finalized at the next sync barrier.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;

use crate::error::StoreError;
use crate::ffi::block;
use crate::rollup::{Aggregate, Rollup};
use crate::wal::{Lsn, Record, RemapEntry, Wal};

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

fn tmp_name(id: u64) -> String {
    format!("{SEG_PREFIX}{id:08}{SEG_SUFFIX}.tmp")
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

pub struct CompactStage {
    pub records: Vec<(Lsn, Record)>,
    pub freed: u64,
    old_id: u64,
    new_id: u64,
    entries: Vec<RemapEntry>,
    dropped: Vec<Lsn>,
    file: File,
}

struct PendingSwap {
    old: u64,
    new: u64,
    file: File,
    entries: Vec<RemapEntry>,
}

struct Inner {
    dir: PathBuf,
    options: StoreOptions,
    wal: Wal,
    segments: HashMap<u64, Arc<File>>,
    index: HashMap<String, (u64, u64, u32, Lsn)>,
    put_locations: HashMap<Lsn, (String, u64, u64, u32)>,
    rollup: Rollup,
    active_id: u64,
    active_offset: u64,
    pending: bool,
    committed_lsn: Lsn,
    last_commit_block: Lsn,
    applied_upto: Lsn,
    dirty_segments: HashSet<u64>,
    sealed: Vec<u64>,
    next_free_id: u64,
    pending_swap: Option<PendingSwap>,
}

struct Snap {
    committed: Lsn,
    index: HashMap<String, (u64, u64, u32, Lsn)>,
    files: HashMap<u64, Arc<File>>,
    put_locations: HashMap<Lsn, (String, u64, u64, u32)>,
    rollup: Rollup,
    dir: PathBuf,
}

/// A lock-free, shareable read handle over a store's committed snapshots.
/// Cloning the handle is cheap; reads never take any lock and never block
/// on the write path.
#[derive(Clone)]
pub struct StoreView {
    snap: Arc<ArcSwap<Snap>>,
}

impl StoreView {
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let snap = self.snap.load_full();
        let Some(&(seg_id, offset, len, _lsn)) = snap.index.get(key) else {
            return Ok(None);
        };
        let file = snap
            .files
            .get(&seg_id)
            .ok_or_else(|| StoreError::Corrupt(format!("segment {} not open", seg_name(seg_id))))?;
        read_value(file, offset, len).map(Some)
    }

    pub fn get_upto(&self, key: &str, committed: Lsn) -> Result<Option<Vec<u8>>, StoreError> {
        let snap = self.snap.load_full();
        let Some(&(seg_id, offset, len, lsn)) = snap.index.get(key) else {
            return Ok(None);
        };
        if lsn > committed {
            return Ok(None);
        }
        let file = snap
            .files
            .get(&seg_id)
            .ok_or_else(|| StoreError::Corrupt(format!("segment {} not open", seg_name(seg_id))))?;
        read_value(file, offset, len).map(Some)
    }

    pub fn capacity(&self, prefix: &str) -> Aggregate {
        self.snap.load_full().rollup.get(prefix)
    }

    /// Committed keys under `prefix`, using the same proper-prefix rule as
    /// the rollup aggregates. Sorted by key.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<(String, u32)> {
        let snap = self.snap.load_full();
        let segs: Vec<&str> = prefix.split('/').filter(|s| !s.is_empty()).collect();
        let mut keys: Vec<(String, u32)> = snap
            .index
            .iter()
            .filter(|(_, (_, _, _, lsn))| *lsn <= snap.committed)
            .filter(|(key, _)| {
                let ksegs: Vec<&str> = key.split('/').filter(|s| !s.is_empty()).collect();
                segs.len() < ksegs.len() && segs.iter().zip(ksegs.iter()).all(|(a, b)| a == b)
            })
            .map(|(key, (_, _, len, _))| (key.clone(), *len))
            .collect();
        keys.sort();
        keys
    }
}

pub struct Store {
    inner: RwLock<Inner>,
    snap: Arc<ArcSwap<Snap>>,
}

impl Snap {
    fn from(inner: &Inner) -> Snap {
        Snap {
            committed: inner.committed_lsn,
            index: inner.index.clone(),
            files: inner.segments.clone(),
            put_locations: inner.put_locations.clone(),
            rollup: inner.rollup.clone(),
            dir: inner.dir.clone(),
        }
    }
}

fn read_value(file: &File, offset: u64, len: u32) -> Result<Vec<u8>, StoreError> {
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

impl Inner {
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
        self.segments.insert(id, Arc::new(file));
        Ok(())
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

    fn seal_active_record(&mut self) -> Result<(Lsn, Record), StoreError> {
        let lsn = self.wal.next_lsn();
        let next = self.next_free_id;
        self.next_free_id = next + 1;
        let rec = Record::SegmentSeal {
            segment_id: self.active_id,
            next_segment_id: next,
        };
        self.wal.append(&rec)?;
        self.pending = true;
        self.sealed.push(self.active_id);
        self.active_id = next;
        self.active_offset = 0;
        Ok((lsn, rec))
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

    fn finalize_pending_swap(&mut self) -> Result<(), StoreError> {
        let Some(swap) = self.pending_swap.take() else {
            return Ok(());
        };
        crate::fault::maybe_fail();
        swap.file.sync_data()?;
        drop(swap.file);
        let tmp_path = self.dir.join(tmp_name(swap.new));
        let final_path = self.dir.join(seg_name(swap.new));
        fs::rename(&tmp_path, &final_path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&final_path)?;
        for e in &swap.entries {
            if let Some((_, _, _, lsn)) = self.index.get(&e.key) {
                if *lsn == e.lsn {
                    self.index
                        .insert(e.key.clone(), (swap.new, e.offset, e.len, e.lsn));
                }
            }
            self.put_locations
                .insert(e.lsn, (e.key.clone(), swap.new, e.offset, e.len));
        }
        self.segments.remove(&swap.old);
        self.segments.insert(swap.new, Arc::new(file));
        self.sealed.retain(|id| *id != swap.old);
        self.sealed.push(swap.new);
        let _ = fs::remove_file(self.dir.join(seg_name(swap.old)));
        Ok(())
    }
}

impl Store {
    pub fn open(dir: &Path) -> Result<Store, StoreError> {
        Self::open_with_options(dir, StoreOptions::default())
    }

    pub fn open_with_options(dir: &Path, options: StoreOptions) -> Result<Store, StoreError> {
        fs::create_dir_all(dir)?;
        let (records, _high_water) = Wal::replay_with_lsns(&dir.join(crate::wal::WAL_FILE))?;
        let wal = Wal::open(dir)?;
        let mut inner = Inner {
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
            sealed: Vec::new(),
            next_free_id: 0,
            pending_swap: None,
        };
        inner.recover(records)?;
        let snap = Arc::new(ArcSwap::from(Arc::new(Snap::from(&inner))));
        Ok(Store {
            inner: RwLock::new(inner),
            snap,
        })
    }

    /// Returns a lock-free read handle over this store's committed
    /// snapshots. The handle stays valid while the store lives; readers
    /// using it never block on writes.
    pub fn read_view(&self) -> StoreView {
        StoreView {
            snap: Arc::clone(&self.snap),
        }
    }

    fn publish(&self, inner: &Inner) {
        self.snap.store(Arc::new(Snap::from(inner)));
    }

    /// Appends `value` under `key` without committing: the `Put` (plus any
    /// `SegmentSeal` and `AggDelta`) records are appended to the WAL buffer
    /// and returned. Durability is the caller's responsibility via
    /// [`Store::sync_wal`] and [`Store::commit`].
    pub fn stage_put(&mut self, key: &str, value: &[u8]) -> Result<StagedPut, StoreError> {
        if value.len() > u32::MAX as usize {
            return Err(StoreError::RecordTooLarge);
        }
        let mut inner = self.inner.write().unwrap();
        let len = value.len() as u64;
        let mut records = Vec::new();
        if inner.active_offset > 0 && inner.active_offset + len > inner.options.segment_max_bytes {
            records.push(inner.seal_active_record()?);
        }
        let active_id = inner.active_id;
        inner.open_segment(active_id)?;
        inner.write_value(value)?;

        let put_lsn = inner.wal.next_lsn();
        let put_rec = Record::Put {
            key: key.to_string(),
            len: value.len() as u32,
            stripe_id: put_lsn,
        };
        inner.wal.append(&put_rec)?;
        records.push((put_lsn, put_rec));

        let agg_lsn = inner.wal.next_lsn();
        let deltas = inner.rollup.deltas_for(key, len);
        let agg_rec = Record::AggDelta { deltas };
        inner.wal.append(&agg_rec)?;
        records.push((agg_lsn, agg_rec));

        let loc = (inner.active_id, inner.active_offset, value.len() as u32);
        inner
            .index
            .insert(key.to_string(), (loc.0, loc.1, loc.2, put_lsn));
        inner
            .put_locations
            .insert(put_lsn, (key.to_string(), loc.0, loc.1, loc.2));
        inner.rollup.record(key, len);
        inner.active_offset += len;
        let active_id = inner.active_id;
        inner.dirty_segments.insert(active_id);
        inner.pending = true;
        if len > inner.options.segment_max_bytes {
            records.push(inner.seal_active_record()?);
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

    /// Rewrites one eligible sealed segment, reclaiming dead values with put
    /// LSN <= `horizon`. Returns `None` if no segment has reclaimable space.
    /// The returned records must be committed (via [`Store::commit`]) and
    /// the stage finalized with [`Store::compact_commit`].
    pub fn compact_stage(&mut self, horizon: Lsn) -> Result<Option<CompactStage>, StoreError> {
        let mut inner = self.inner.write().unwrap();
        let mut chosen: Option<u64> = None;
        for sid in &inner.sealed {
            let live: HashSet<Lsn> = inner
                .index
                .values()
                .filter(|(s, _, _, _)| *s == *sid)
                .map(|(_, _, _, l)| *l)
                .collect();
            let reclaim = inner
                .put_locations
                .iter()
                .filter(|(lsn, (_, seg, _, _len))| {
                    *seg == *sid && !live.contains(lsn) && **lsn <= horizon
                })
                .map(|(_, (_, _, _, len))| *len as u64)
                .sum::<u64>();
            if reclaim > 0 {
                chosen = Some(*sid);
                break;
            }
        }
        let Some(old_id) = chosen else {
            return Ok(None);
        };

        let live: HashSet<Lsn> = inner
            .index
            .values()
            .filter(|(s, _, _, _)| *s == old_id)
            .map(|(_, _, _, l)| *l)
            .collect();
        let mut keep: Vec<(Lsn, String, u64, u32)> = inner
            .put_locations
            .iter()
            .filter(|(lsn, (_, seg, _, _))| {
                *seg == old_id && (live.contains(lsn) || **lsn > horizon)
            })
            .map(|(lsn, (key, _, off, len))| (*lsn, key.clone(), *off, *len))
            .collect();
        keep.sort_by_key(|(_, _, off, _)| *off);

        let new_id = inner.next_free_id;
        inner.next_free_id += 1;

        let tmp_path = inner.dir.join(tmp_name(new_id));
        let final_path = inner.dir.join(seg_name(new_id));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        let old_file = inner
            .segments
            .get(&old_id)
            .cloned()
            .ok_or_else(|| StoreError::Corrupt(format!("segment {old_id} not open")))?;

        let mut new_offset = 0u64;
        let mut entries = Vec::with_capacity(keep.len());
        for (lsn, key, old_off, len) in &keep {
            let mut value = vec![0u8; *len as usize];
            let mut filled = 0usize;
            while filled < value.len() {
                let n = old_file.read_at(&mut value[filled..], old_off + filled as u64)?;
                if n == 0 {
                    return Err(StoreError::Corrupt("short read during compaction".into()));
                }
                filled += n;
            }
            let mut written = 0usize;
            while written < value.len() {
                let n = file.write_at(&value[written..], new_offset + written as u64)?;
                if n == 0 {
                    return Err(StoreError::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "short write during compaction",
                    )));
                }
                written += n;
            }
            entries.push(RemapEntry {
                key: key.clone(),
                lsn: *lsn,
                offset: new_offset,
                len: *len,
            });
            new_offset += *len as u64;
        }
        crate::fault::maybe_fail();
        file.sync_data()?;
        drop(file);
        fs::rename(&tmp_path, &final_path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&final_path)?;

        let mut dropped = Vec::new();
        let mut freed = 0u64;
        for (lsn, (_, seg, _, len)) in &inner.put_locations {
            if *seg == old_id && !keep.iter().any(|(l, ..)| l == lsn) {
                dropped.push(*lsn);
                freed += *len as u64;
            }
        }

        let mut records = Vec::new();
        let swap_lsn = inner.wal.next_lsn();
        let swap_rec = Record::SegmentSwap {
            old_segment_id: old_id,
            new_segment_id: new_id,
        };
        inner.wal.append(&swap_rec)?;
        records.push((swap_lsn, swap_rec));
        inner.pending = true;

        let mut i = 0;
        while i < entries.len() {
            let mut budget = block::PAYLOAD_MAX - 1 - 8 - 2;
            let mut j = i;
            while j < entries.len() {
                let cost = 2 + entries[j].key.len() + 20;
                if cost > budget {
                    break;
                }
                budget -= cost;
                j += 1;
            }
            let chunk = entries[i..j].to_vec();
            let lsn = inner.wal.next_lsn();
            let rec = Record::SegmentRemap {
                new_segment_id: new_id,
                entries: chunk,
            };
            inner.wal.append(&rec)?;
            records.push((lsn, rec));
            inner.pending = true;
            i = j;
        }

        Ok(Some(CompactStage {
            records,
            freed,
            old_id,
            new_id,
            entries,
            dropped,
            file,
        }))
    }

    /// Finalizes a committed compaction: repoints the index and
    /// put-location map at the replacement segment, drops the old file, and
    /// returns the number of reclaimed bytes.
    pub fn compact_commit(&mut self, stage: CompactStage) -> Result<u64, StoreError> {
        let mut inner = self.inner.write().unwrap();
        let freed = stage.freed;
        let old_id = stage.old_id;
        let new_id = stage.new_id;
        for e in &stage.entries {
            if let Some((_, _, _, lsn)) = inner.index.get(&e.key) {
                if *lsn == e.lsn {
                    inner
                        .index
                        .insert(e.key.clone(), (new_id, e.offset, e.len, e.lsn));
                }
            }
            inner
                .put_locations
                .insert(e.lsn, (e.key.clone(), new_id, e.offset, e.len));
        }
        for lsn in &stage.dropped {
            inner.put_locations.remove(lsn);
        }
        inner.segments.remove(&old_id);
        inner.segments.insert(new_id, Arc::new(stage.file));
        inner.sealed.retain(|id| *id != old_id);
        inner.sealed.push(new_id);
        let _ = fs::remove_file(inner.dir.join(seg_name(old_id)));
        self.publish(&inner);
        Ok(freed)
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
        let mut inner = self.inner.write().unwrap();
        inner.applied_upto = inner.applied_upto.max(lsn);
        match rec {
            Record::Put { key, len, .. } => {
                let data = data.ok_or_else(|| {
                    StoreError::Corrupt("Put record without value data".to_string())
                })?;
                if data.len() != *len as usize {
                    return Err(StoreError::Corrupt("Put value length mismatch".to_string()));
                }
                let active_id = inner.active_id;
                inner.open_segment(active_id)?;
                inner.write_value(data)?;
                inner.wal.append_at(rec, lsn)?;
                let loc = (active_id, inner.active_offset, *len);
                inner.index.insert(key.clone(), (loc.0, loc.1, loc.2, lsn));
                inner
                    .put_locations
                    .insert(lsn, (key.clone(), loc.0, loc.1, loc.2));
                inner.rollup.record(key, data.len() as u64);
                inner.active_offset += data.len() as u64;
                let active_id = inner.active_id;
                inner.dirty_segments.insert(active_id);
                inner.pending = true;
            }
            Record::SegmentSeal {
                segment_id,
                next_segment_id,
            } => {
                if *segment_id != inner.active_id {
                    return Err(StoreError::Corrupt(format!(
                        "seal for segment {} but follower active segment is {}",
                        segment_id, inner.active_id
                    )));
                }
                inner.wal.append_at(rec, lsn)?;
                let sealed_id = inner.active_id;
                inner.sealed.push(sealed_id);
                inner.active_id = *next_segment_id;
                inner.active_offset = 0;
                inner.next_free_id = inner.next_free_id.max(inner.active_id + 1);
                inner.pending = true;
            }
            Record::AggDelta { .. } => {
                inner.wal.append_at(rec, lsn)?;
                inner.pending = true;
            }
            Record::SegmentSwap {
                old_segment_id,
                new_segment_id,
            } => {
                if inner.pending_swap.is_some() {
                    return Err(StoreError::Corrupt("overlapping segment swap".to_string()));
                }
                let tmp_path = inner.dir.join(tmp_name(*new_segment_id));
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&tmp_path)?;
                inner.wal.append_at(rec, lsn)?;
                inner.pending_swap = Some(PendingSwap {
                    old: *old_segment_id,
                    new: *new_segment_id,
                    file,
                    entries: Vec::new(),
                });
                inner.pending = true;
            }
            Record::SegmentRemap {
                new_segment_id,
                entries,
            } => {
                inner.wal.append_at(rec, lsn)?;
                let mut swap = inner
                    .pending_swap
                    .take()
                    .ok_or_else(|| StoreError::Corrupt("remap without swap".to_string()))?;
                if swap.new != *new_segment_id {
                    inner.pending_swap = Some(swap);
                    return Err(StoreError::Corrupt("remap target mismatch".to_string()));
                }
                for e in entries {
                    let (_, seg, off, len) =
                        inner.put_locations.get(&e.lsn).cloned().ok_or_else(|| {
                            StoreError::Corrupt(format!("remap source missing for lsn {}", e.lsn))
                        })?;
                    if seg != swap.old || len != e.len {
                        inner.pending_swap = Some(swap);
                        return Err(StoreError::Corrupt("remap source mismatch".to_string()));
                    }
                    let file = inner
                        .segments
                        .get(&seg)
                        .cloned()
                        .ok_or_else(|| StoreError::Corrupt("segment not open".to_string()))?;
                    let value = read_value(&file, off, len)?;
                    let mut written = 0usize;
                    while written < value.len() {
                        let n = swap
                            .file
                            .write_at(&value[written..], e.offset + written as u64)?;
                        if n == 0 {
                            inner.pending_swap = Some(swap);
                            return Err(StoreError::Io(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "short write during swap",
                            )));
                        }
                        written += n;
                    }
                    swap.entries.push(e.clone());
                }
                inner.pending_swap = Some(swap);
                inner.pending = true;
            }
            Record::Commit { lsn: committed } => {
                inner.wal.append_at(rec, lsn)?;
                inner.committed_lsn = inner.committed_lsn.max(*committed);
                inner.last_commit_block = inner.last_commit_block.max(lsn);
                inner.pending = true;
            }
        }
        Ok(())
    }

    /// Fsyncs the WAL buffer without appending a commit record.
    pub fn sync_wal(&mut self) -> Result<(), StoreError> {
        self.inner.write().unwrap().wal.sync()
    }

    /// Fsyncs dirty segment files and the WAL (follower durability point).
    /// Pending segment swaps are finalized first, so a durable WAL always
    /// points at replacement files that exist on disk.
    pub fn sync_all(&mut self) -> Result<(), StoreError> {
        let mut inner = self.inner.write().unwrap();
        inner.finalize_pending_swap()?;
        inner.fsync_dirty_segments()?;
        inner.wal.sync()?;
        inner.dirty_segments.clear();
        self.publish(&inner);
        Ok(())
    }

    /// Commit protocol: fsync dirty segments, append `Commit { lsn }`, and
    /// fsync the WAL. All records with LSN <= `lsn` become durable and a
    /// new reader snapshot is published.
    pub fn commit(&mut self, lsn: Lsn) -> Result<(), StoreError> {
        let mut inner = self.inner.write().unwrap();
        if !inner.pending {
            return Ok(());
        }
        inner.fsync_dirty_segments()?;
        inner.wal.append(&Record::Commit { lsn })?;
        inner.wal.sync()?;
        let commit_block = inner.wal.next_lsn() - 1;
        inner.committed_lsn = inner.committed_lsn.max(lsn);
        inner.last_commit_block = inner.last_commit_block.max(commit_block);
        inner.applied_upto = inner.applied_upto.max(commit_block);
        inner.pending = false;
        inner.dirty_segments.clear();
        self.publish(&inner);
        Ok(())
    }

    /// Commits everything currently staged.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        let lsn = {
            let inner = self.inner.read().unwrap();
            if !inner.pending {
                return Ok(());
            }
            inner.wal.next_lsn() - 1
        };
        self.commit(lsn)
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let snap = self.snap.load_full();
        let Some(&(seg_id, offset, len, _lsn)) = snap.index.get(key) else {
            return Ok(None);
        };
        let file = snap
            .files
            .get(&seg_id)
            .ok_or_else(|| StoreError::Corrupt(format!("segment {} not open", seg_name(seg_id))))?;
        read_value(file, offset, len).map(Some)
    }

    /// Like [`Store::get`], but only returns values whose put LSN is at or
    /// below `committed`. This is the read-your-writes gate used by the
    /// leader to serve reads only up to its commit index.
    pub fn get_upto(&self, key: &str, committed: Lsn) -> Result<Option<Vec<u8>>, StoreError> {
        let snap = self.snap.load_full();
        let Some(&(seg_id, offset, len, lsn)) = snap.index.get(key) else {
            return Ok(None);
        };
        if lsn > committed {
            return Ok(None);
        }
        let file = snap
            .files
            .get(&seg_id)
            .ok_or_else(|| StoreError::Corrupt(format!("segment {} not open", seg_name(seg_id))))?;
        read_value(file, offset, len).map(Some)
    }

    /// Reads the value written by the put at `lsn` (used for backfill).
    pub fn value_at(&self, lsn: Lsn) -> Result<Vec<u8>, StoreError> {
        let snap = self.snap.load_full();
        let (_, seg_id, offset, len) = snap
            .put_locations
            .get(&lsn)
            .cloned()
            .ok_or_else(|| StoreError::Corrupt(format!("no put at lsn {lsn}")))?;
        let file = snap
            .files
            .get(&seg_id)
            .ok_or_else(|| StoreError::Corrupt(format!("segment {} not open", seg_name(seg_id))))?;
        read_value(file, offset, len)
    }

    /// Reads the leader's WAL records (for backfill).
    pub fn scan_wal(&self) -> Result<Vec<(Lsn, Record)>, StoreError> {
        let snap = self.snap.load_full();
        Wal::scan(&snap.dir.join(crate::wal::WAL_FILE))
    }

    pub fn next_lsn(&self) -> Lsn {
        self.inner.read().unwrap().wal.next_lsn()
    }

    pub fn committed_lsn(&self) -> Lsn {
        self.snap.load_full().committed
    }

    /// Block LSN of the most recent durable `Commit` record. A follower
    /// asks the leader for records strictly above this.
    pub fn last_commit_block(&self) -> Lsn {
        self.inner.read().unwrap().last_commit_block
    }

    /// Highest record LSN that has been applied (durable or not).
    pub fn applied_upto(&self) -> Lsn {
        self.inner.read().unwrap().applied_upto
    }

    pub fn keys(&self) -> Vec<String> {
        self.snap.load_full().index.keys().cloned().collect()
    }

    pub fn committed_keys(&self) -> Vec<(String, u32)> {
        let snap = self.snap.load_full();
        snap.index
            .iter()
            .filter(|(_, (_, _, _, lsn))| *lsn <= snap.committed)
            .map(|(k, (_, _, len, _))| (k.clone(), *len))
            .collect()
    }

    pub fn capacity(&self, prefix: &str) -> Aggregate {
        self.snap.load_full().rollup.get(prefix)
    }

    /// Committed keys under `prefix`, using the same proper-prefix rule as
    /// the rollup aggregates. Sorted by key.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<(String, u32)> {
        let snap = self.snap.load_full();
        let segs: Vec<&str> = prefix.split('/').filter(|s| !s.is_empty()).collect();
        let mut keys: Vec<(String, u32)> = snap
            .index
            .iter()
            .filter(|(_, (_, _, _, lsn))| *lsn <= snap.committed)
            .filter(|(key, _)| {
                let ksegs: Vec<&str> = key.split('/').filter(|s| !s.is_empty()).collect();
                segs.len() < ksegs.len() && segs.iter().zip(ksegs.iter()).all(|(a, b)| a == b)
            })
            .map(|(key, (_, _, len, _))| (key.clone(), *len))
            .collect();
        keys.sort();
        keys
    }
}

impl Inner {
    fn recover(&mut self, records: Vec<(Lsn, Record)>) -> Result<(), StoreError> {
        let mut committed: HashMap<u64, u64> = HashMap::new();
        let mut swap_pairs: Vec<(u64, u64)> = Vec::new();
        let mut remap_entries: HashMap<u64, Vec<RemapEntry>> = HashMap::new();
        for (lsn, rec) in &records {
            self.applied_upto = self.applied_upto.max(*lsn);
            match rec {
                Record::Put { key, len, .. } => {
                    let len64 = *len as u64;
                    let loc = (self.active_id, self.active_offset, *len);
                    self.index.insert(key.clone(), (loc.0, loc.1, loc.2, *lsn));
                    self.put_locations
                        .insert(*lsn, (key.clone(), loc.0, loc.1, loc.2));
                    self.rollup.record(key, len64);
                    self.active_offset += len64;
                    committed.insert(self.active_id, self.active_offset);
                }
                Record::SegmentSeal {
                    segment_id,
                    next_segment_id,
                } => {
                    committed.insert(*segment_id, self.active_offset);
                    self.sealed.push(*segment_id);
                    self.active_id = *next_segment_id;
                    self.active_offset = 0;
                }
                Record::SegmentSwap {
                    old_segment_id,
                    new_segment_id,
                } => {
                    swap_pairs.push((*old_segment_id, *new_segment_id));
                }
                Record::SegmentRemap {
                    new_segment_id,
                    entries,
                } => {
                    remap_entries
                        .entry(*new_segment_id)
                        .or_default()
                        .extend(entries.iter().cloned());
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

        let mut to_delete: Vec<u64> = Vec::new();
        for (old, new) in swap_pairs {
            let mut new_len = 0u64;
            if let Some(entries) = remap_entries.get(&new) {
                for e in entries {
                    if let Some((_, _, _, lsn)) = self.index.get(&e.key) {
                        if *lsn == e.lsn {
                            self.index
                                .insert(e.key.clone(), (new, e.offset, e.len, e.lsn));
                        }
                    }
                    self.put_locations
                        .insert(e.lsn, (e.key.clone(), new, e.offset, e.len));
                    new_len = new_len.max(e.offset + e.len as u64);
                }
            }
            committed.insert(new, new_len);
            committed.insert(old, 0);
            self.sealed.retain(|id| *id != old);
            self.sealed.push(new);
            to_delete.push(old);
        }

        let mut existing: HashMap<u64, PathBuf> = HashMap::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy().into_owned();
            if name.ends_with(".tmp") {
                fs::remove_file(entry.path())?;
                continue;
            }
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
        for id in to_delete {
            let _ = fs::remove_file(self.dir.join(seg_name(id)));
        }
        let mut max_id = self.active_id;
        for id in committed.keys().chain(existing.keys()) {
            max_id = max_id.max(*id);
        }
        self.next_free_id = max_id + 1;
        for id in committed.keys() {
            self.open_segment(*id)?;
        }
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort durability on clean shutdown; callers that need to know
        // the outcome must call `flush` explicitly.
        let _ = self.flush();
    }
}
