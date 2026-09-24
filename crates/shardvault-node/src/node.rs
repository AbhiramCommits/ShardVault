//! Single-leader log replication.
//!
//! The leader stages each PUT in its WAL, fsyncs it, ships the records to
//! every follower, and waits for a majority of fsync ACKs (counting itself)
//! before appending the `Commit` record and replying to the client. A PUT
//! is only acknowledged after its commit is durable; if a majority cannot
//! be reached, the PUT fails instead. Reads are served only up to the
//! commit index, so a GET after an ACKed PUT always sees the new value.
//!
//! Followers apply replicated records at the leader's LSNs, so their WAL
//! and segment layout match the leader's. A follower that falls behind (or
//! restarts) sends `NeedFrom` and the leader streams a backfill of the
//! records it missed, plus the current commit index.

use std::collections::VecDeque;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use shardvault_core::fault;
use shardvault_core::segment::{Store, StoreOptions};
use shardvault_core::wal::{Lsn, Record};

use crate::protocol::{recv_frame, send_frame, Frame, ROLE_FOLLOWER, ROLE_LEADER};

fn store_options() -> StoreOptions {
    let mut opts = StoreOptions::default();
    if let Ok(v) = std::env::var("SV_SEGMENT_MAX_BYTES") {
        if let Ok(n) = v.parse::<u64>() {
            opts.segment_max_bytes = n;
        }
    }
    opts
}

pub struct Config {
    pub id: u64,
    pub addr: String,
    pub peers: Vec<String>,
    pub dir: PathBuf,
    pub role: u8,
}

fn store_err(e: shardvault_core::error::StoreError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

fn log_err(what: &str, e: impl std::fmt::Display) {
    eprintln!("shardvault-node: {what}: {e}");
}

pub fn run(cfg: Config) -> io::Result<()> {
    match cfg.role {
        ROLE_LEADER => run_leader(cfg),
        ROLE_FOLLOWER => run_follower(cfg),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown role {other}"),
        )),
    }
}

type FrameQueue = Arc<(Mutex<VecDeque<Frame>>, Condvar)>;

struct FollowerLink {
    id: u64,
    addr: String,
    match_index: AtomicU64,
    committed_through: AtomicU64,
    alive: AtomicBool,
    queue: FrameQueue,
}

fn enqueue(queue: &FrameQueue, frames: Vec<Frame>) {
    let (lock, cvar) = &**queue;
    let mut q = lock.lock().unwrap();
    q.extend(frames);
    cvar.notify_all();
}

struct Leader {
    store: Arc<Mutex<Store>>,
    commit_index: AtomicU64,
    followers: Vec<Arc<FollowerLink>>,
    failed: Mutex<std::collections::HashSet<u64>>,
    quorum: u64,
    wake: Arc<(Mutex<()>, Condvar)>,
}

impl Leader {
    fn new(cfg: &Config) -> io::Result<Leader> {
        let store = Store::open_with_options(&cfg.dir, store_options()).map_err(store_err)?;
        let store = Arc::new(Mutex::new(store));
        let commit_index = AtomicU64::new(store.lock().unwrap().next_lsn() - 1);
        let quorum = ((cfg.peers.len() + 1) / 2) as u64;
        let mut followers = Vec::new();
        for (i, addr) in cfg.peers.iter().enumerate() {
            if i as u64 == cfg.id {
                continue;
            }
            followers.push(Arc::new(FollowerLink {
                id: i as u64,
                addr: addr.clone(),
                match_index: AtomicU64::new(0),
                committed_through: AtomicU64::new(0),
                alive: AtomicBool::new(false),
                queue: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
            }));
        }
        Ok(Leader {
            store,
            commit_index,
            followers,
            failed: Mutex::new(std::collections::HashSet::new()),
            quorum,
            wake: Arc::new((Mutex::new(()), Condvar::new())),
        })
    }

    fn send_batch(&self, records: &[(Lsn, Record)], value: Option<&[u8]>, end: Lsn) {
        for f in &self.followers {
            let mut frames = Vec::with_capacity(records.len() + 1);
            for (lsn, rec) in records {
                let data = if matches!(rec, Record::Put { .. }) {
                    value.map(|v| v.to_vec())
                } else {
                    None
                };
                frames.push(Frame::Append {
                    lsn: *lsn,
                    rec: rec.clone(),
                    data,
                });
            }
            frames.push(Frame::SyncBarrier { lsn: end });
            enqueue(&f.queue, frames);
        }
    }

    fn process_put(&self, key: String, value: Vec<u8>) -> Result<(), String> {
        let staged = {
            let mut store = self.store.lock().unwrap();
            store.stage_put(&key, &value).map_err(|e| e.to_string())?
        };
        let batch_end = staged.records.last().map(|(lsn, _)| *lsn).unwrap();
        {
            let mut store = self.store.lock().unwrap();
            store.sync_wal().map_err(|e| e.to_string())?;
        }
        self.send_batch(&staged.records, Some(&value), batch_end);
        self.await_quorum(batch_end)?;
        let commit_block;
        {
            let mut store = self.store.lock().unwrap();
            store.commit(batch_end).map_err(|e| e.to_string())?;
            commit_block = store.next_lsn() - 1;
        }
        self.commit_index.store(commit_block, Ordering::SeqCst);
        let commit_records = [(commit_block, Record::Commit { lsn: batch_end })];
        self.send_batch(&commit_records, None, commit_block);
        Ok(())
    }

    fn process_compact(&self) -> Result<u64, String> {
        let commit_index = self.commit_index.load(Ordering::SeqCst);
        let horizon = if self.followers.is_empty() {
            commit_index
        } else {
            self.followers
                .iter()
                .map(|f| f.committed_through.load(Ordering::SeqCst))
                .min()
                .unwrap_or(0)
        };
        let staged = {
            let mut store = self.store.lock().unwrap();
            match store.compact_stage(horizon).map_err(|e| e.to_string())? {
                Some(staged) => staged,
                None => return Ok(0),
            }
        };
        let batch_end = staged.records.last().map(|(lsn, _)| *lsn).unwrap();
        {
            let mut store = self.store.lock().unwrap();
            store.sync_wal().map_err(|e| e.to_string())?;
        }
        self.send_batch(&staged.records, None, batch_end);
        self.await_quorum(batch_end)?;
        let commit_block;
        {
            let mut store = self.store.lock().unwrap();
            store.commit(batch_end).map_err(|e| e.to_string())?;
            commit_block = store.next_lsn() - 1;
        }
        let freed = {
            let mut store = self.store.lock().unwrap();
            store.compact_commit(staged).map_err(|e| e.to_string())?
        };
        self.commit_index.store(commit_block, Ordering::SeqCst);
        let commit_records = [(commit_block, Record::Commit { lsn: batch_end })];
        self.send_batch(&commit_records, None, commit_block);
        Ok(freed)
    }

    fn await_quorum(&self, batch_end: Lsn) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_millis(1500);
        loop {
            let mut acks = 1u64;
            for f in &self.followers {
                if f.match_index.load(Ordering::SeqCst) >= batch_end {
                    acks += 1;
                }
            }
            if acks >= self.quorum {
                return Ok(());
            }
            let reachable = 1 + self
                .followers
                .iter()
                .filter(|f| f.alive.load(Ordering::SeqCst))
                .count() as u64;
            if reachable < self.quorum && Instant::now() > deadline {
                return Err("quorum lost".to_string());
            }
            let (lock, cvar) = &*self.wake;
            let guard = lock.lock().unwrap();
            let (guard, _) = cvar
                .wait_timeout(guard, Duration::from_millis(20))
                .map_err(|e| e.to_string())?;
            drop(guard);
        }
    }

    /// Enqueues the records in `[from, commit_index]` (plus a barrier) for a
    /// follower that fell behind. The queue is cleared first so stale live
    /// frames cannot be written ahead of the backfill.
    fn handle_backfill(&self, f: &FollowerLink, from: Lsn) {
        let commit = self.commit_index.load(Ordering::SeqCst);
        let records = match self.store.lock().unwrap().scan_wal() {
            Ok(r) => r,
            Err(e) => {
                log_err("backfill scan", e);
                return;
            }
        };
        let mut frames = Vec::new();
        for (lsn, rec) in records {
            if lsn < from || lsn > commit {
                continue;
            }
            let data = if matches!(rec, Record::Put { .. }) {
                match self.store.lock().unwrap().value_at(lsn) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        log_err("backfill value", e);
                        return;
                    }
                }
            } else {
                None
            };
            frames.push(Frame::Append { lsn, rec, data });
        }
        frames.push(Frame::SyncBarrier { lsn: commit });
        let (lock, cvar) = &*f.queue;
        let mut q = lock.lock().unwrap();
        // Live frames with LSNs above the backfill's commit point must be
        // written AFTER the backfill records, in LSN order; frames at or
        // below the commit point are covered by the backfill and dropped.
        let retained: Vec<Frame> = q
            .drain(..)
            .filter(|frame| match frame {
                Frame::Append { lsn, .. } | Frame::SyncBarrier { lsn } => *lsn > commit,
                _ => false,
            })
            .collect();
        q.extend(frames);
        q.extend(retained);
        cvar.notify_all();
    }

    fn notify_ack(&self) {
        let (lock, cvar) = &*self.wake;
        let _g = lock.lock().unwrap();
        cvar.notify_all();
    }

    fn run(self: Arc<Self>, addr: &str) -> io::Result<()> {
        let listener = TcpListener::bind(addr)?;
        let bound = listener.local_addr()?;
        println!("listening on {bound}");
        io::stdout().flush()?;
        for f in &self.followers {
            let me = Arc::clone(&self);
            let f = Arc::clone(f);
            thread::spawn(move || me.dialer_loop(f));
        }
        for stream in listener.incoming() {
            let stream = stream?;
            let me = Arc::clone(&self);
            thread::spawn(move || me.handle_client(stream));
        }
        Ok(())
    }

    fn writer_loop(queue: FrameQueue, open: Arc<AtomicBool>, mut writer: BufWriter<TcpStream>) {
        loop {
            let frame = {
                let (lock, cvar) = &*queue;
                let mut q = lock.lock().unwrap();
                loop {
                    if let Some(f) = q.pop_front() {
                        break Some(f);
                    }
                    if !open.load(Ordering::SeqCst) {
                        break None;
                    }
                    let (guard, _) = cvar.wait_timeout(q, Duration::from_millis(100)).unwrap();
                    q = guard;
                }
            };
            match frame {
                Some(frame) => {
                    if send_frame(&mut writer, &frame).is_err() {
                        return;
                    }
                }
                None => return,
            }
        }
    }

    fn dialer_loop(self: Arc<Self>, f: Arc<FollowerLink>) {
        loop {
            if self.failed.lock().unwrap().contains(&f.id) {
                thread::sleep(Duration::from_millis(200));
                continue;
            }
            let stream = match TcpStream::connect(&f.addr) {
                Ok(s) => s,
                Err(_) => {
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
            };
            let mut writer = match stream.try_clone() {
                Ok(s) => BufWriter::new(s),
                Err(_) => continue,
            };
            let mut reader = BufReader::new(match stream.try_clone() {
                Ok(s) => s,
                Err(_) => continue,
            });
            if send_frame(
                &mut writer,
                &Frame::Hello {
                    id: f.id,
                    role: ROLE_LEADER,
                },
            )
            .is_err()
            {
                continue;
            }
            match recv_frame(&mut reader) {
                Ok(Frame::NeedFrom { lsn }) => self.handle_backfill(&f, lsn),
                _ => continue,
            }
            f.alive.store(true, Ordering::SeqCst);
            let open = Arc::new(AtomicBool::new(true));
            let wqueue = Arc::clone(&f.queue);
            let wopen = Arc::clone(&open);
            let writer_thread = thread::spawn(move || Self::writer_loop(wqueue, wopen, writer));
            let result = (|| loop {
                match recv_frame(&mut reader) {
                    Ok(Frame::SyncAck { lsn }) => {
                        f.match_index.fetch_max(lsn, Ordering::SeqCst);
                        let ci = self.commit_index.load(Ordering::SeqCst);
                        if lsn >= ci {
                            f.committed_through.store(ci, Ordering::SeqCst);
                        }
                        self.notify_ack();
                    }
                    Ok(Frame::NeedFrom { lsn }) => {
                        self.handle_backfill(&f, lsn);
                    }
                    Ok(_) => {}
                    Err(e) => return e,
                }
            })();
            open.store(false, Ordering::SeqCst);
            let _ = stream.shutdown(Shutdown::Both);
            let _ = writer_thread.join();
            f.alive.store(false, Ordering::SeqCst);
            let _ = result;
        }
    }

    fn handle_client(&self, stream: TcpStream) {
        let mut writer = match stream.try_clone() {
            Ok(s) => BufWriter::new(s),
            Err(_) => return,
        };
        let mut reader = BufReader::new(stream);
        loop {
            let frame = match recv_frame(&mut reader) {
                Ok(f) => f,
                Err(_) => return,
            };
            match frame {
                Frame::PutReq { id, key, value } => {
                    let resp = match self.process_put(key, value) {
                        Ok(()) => Frame::PutOk { id },
                        Err(msg) => Frame::PutErr { id, msg },
                    };
                    if send_frame(&mut writer, &resp).is_err() {
                        return;
                    }
                }
                Frame::GetReq { id, key } => {
                    let committed = self.commit_index.load(Ordering::SeqCst);
                    let resp = match self.store.lock().unwrap().get_upto(&key, committed) {
                        Ok(v) => Frame::GetOk { id, value: v },
                        Err(_) => Frame::GetOk { id, value: None },
                    };
                    if send_frame(&mut writer, &resp).is_err() {
                        return;
                    }
                }
                Frame::StatusReq => {
                    let matches = self
                        .followers
                        .iter()
                        .map(|f| (f.id, f.match_index.load(Ordering::SeqCst)))
                        .collect();
                    let resp = Frame::StatusOk {
                        commit_index: self.commit_index.load(Ordering::SeqCst),
                        matches,
                    };
                    if send_frame(&mut writer, &resp).is_err() {
                        return;
                    }
                }
                Frame::ProbeFsync => {
                    let resp = Frame::ProbeFsyncResp {
                        count: fault::count(),
                    };
                    if send_frame(&mut writer, &resp).is_err() {
                        return;
                    }
                }
                Frame::Compact => {
                    let resp = match self.process_compact() {
                        Ok(freed) => Frame::CompactDone { freed },
                        Err(msg) => {
                            eprintln!("shardvault-node: compaction failed: {msg}");
                            Frame::CompactDone { freed: 0 }
                        }
                    };
                    if send_frame(&mut writer, &resp).is_err() {
                        return;
                    }
                }
                Frame::ListReq { id, prefix } => {
                    let keys = self.store.lock().unwrap().keys_with_prefix(&prefix);
                    let resp = Frame::ListOk { id, keys };
                    if send_frame(&mut writer, &resp).is_err() {
                        return;
                    }
                }
                Frame::CapacityReq { id, prefix } => {
                    let store = self.store.lock().unwrap();
                    let agg = store.capacity(&prefix);
                    let keys = store.keys_with_prefix(&prefix);
                    let brute = keys
                        .iter()
                        .fold((0u64, 0u64), |(c, b), (_, len)| (c + 1, b + *len as u64));
                    let resp = Frame::CapacityOk {
                        id,
                        aggregate: (agg.object_count, agg.byte_count),
                        brute,
                    };
                    if send_frame(&mut writer, &resp).is_err() {
                        return;
                    }
                }
                Frame::SimulateFailure { node_id } => {
                    self.failed.lock().unwrap().insert(node_id);
                    for f in &self.followers {
                        if f.id == node_id {
                            f.alive.store(false, Ordering::SeqCst);
                        }
                    }
                    if send_frame(&mut writer, &Frame::SimulateFailureDone).is_err() {
                        return;
                    }
                }
                _ => {}
            }
        }
    }
}

fn run_leader(cfg: Config) -> io::Result<()> {
    let addr = cfg.addr.clone();
    let leader = Arc::new(Leader::new(&cfg)?);
    leader.run(&addr)
}

struct FollowerRunner {
    current: Arc<Mutex<Option<TcpStream>>>,
    new_streams: mpsc::Sender<TcpStream>,
}

impl FollowerRunner {
    fn new(dir: &Path) -> io::Result<FollowerRunner> {
        let store = Arc::new(Mutex::new(
            Store::open_with_options(dir, store_options()).map_err(store_err)?,
        ));
        let (tx, rx) = mpsc::channel::<TcpStream>();
        let runner = FollowerRunner {
            current: Arc::new(Mutex::new(None)),
            new_streams: tx,
        };
        thread::spawn(move || apply_loop(store, rx));
        Ok(runner)
    }

    fn on_leader_connect(&self, stream: TcpStream) -> io::Result<()> {
        let mut cur = self.current.lock().unwrap();
        if let Some(old) = cur.take() {
            let _ = old.shutdown(Shutdown::Both);
        }
        let dup = stream.try_clone()?;
        *cur = Some(stream);
        if self.new_streams.send(dup).is_err() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "apply loop gone"));
        }
        Ok(())
    }
}

fn apply_loop(store: Arc<Mutex<Store>>, rx: mpsc::Receiver<TcpStream>) {
    for stream in rx {
        let mut writer = match stream.try_clone() {
            Ok(s) => BufWriter::new(s),
            Err(_) => continue,
        };
        let mut reader = BufReader::new(stream);
        let from = store.lock().unwrap().last_commit_block() + 1;
        if send_frame(&mut writer, &Frame::NeedFrom { lsn: from }).is_err() {
            continue;
        }
        while let Ok(frame) = recv_frame(&mut reader) {
            match frame {
                Frame::Append { lsn, rec, data } => {
                    let mut store = store.lock().unwrap();
                    if lsn <= store.last_commit_block() {
                        continue;
                    }
                    if let Err(e) = store.apply_record(lsn, &rec, data.as_deref()) {
                        log_err("apply", e);
                        break;
                    }
                }
                Frame::SyncBarrier { .. } => {
                    let ack = {
                        let mut store = store.lock().unwrap();
                        if let Err(e) = store.sync_all() {
                            log_err("sync_all", e);
                            break;
                        }
                        store.applied_upto()
                    };
                    if send_frame(&mut writer, &Frame::SyncAck { lsn: ack }).is_err() {
                        break;
                    }
                }
                _ => {}
            }
        }
    }
}

fn run_follower(cfg: Config) -> io::Result<()> {
    let runner = FollowerRunner::new(&cfg.dir)?;
    let listener = TcpListener::bind(&cfg.addr)?;
    let bound = listener.local_addr()?;
    println!("listening on {bound}");
    io::stdout().flush()?;
    for stream in listener.incoming() {
        let stream = stream?;
        let mut reader = BufReader::new(stream.try_clone()?);
        if let Ok(Frame::Hello {
            role: ROLE_LEADER, ..
        }) = recv_frame(&mut reader)
        {
            runner.on_leader_connect(stream)?;
        }
    }
    Ok(())
}
