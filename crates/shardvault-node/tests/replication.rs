use shardvault_core::segment::Store;
use shardvault_node::protocol::{recv_frame, send_frame, Frame};
use std::io::{BufRead, BufReader, BufWriter};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!("shardvault-{name}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

struct ChildGuard(Child);

impl ChildGuard {
    fn spawn(
        id: u64,
        peers: &[String],
        addr: &str,
        dir: &std::path::Path,
        role: &str,
    ) -> ChildGuard {
        let exe = env!("CARGO_BIN_EXE_shardvault-node");
        let mut cmd = Command::new(exe);
        cmd.args([
            "--id",
            &id.to_string(),
            "--peers",
            &peers.join(","),
            "--addr",
            addr,
            "--dir",
            dir.to_str().unwrap(),
            "--role",
            role,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
        ChildGuard(cmd.spawn().unwrap())
    }

    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }

    fn try_wait(&mut self) -> Option<std::process::ExitStatus> {
        self.0.try_wait().unwrap()
    }

    fn wait_listening(&mut self) -> String {
        let stdout = self.0.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    assert!(
                        Instant::now() < deadline,
                        "node exited before listening: {:?}",
                        self.try_wait()
                    );
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(_) if line.contains("listening on") => {
                    break line
                        .trim()
                        .trim_start_matches("listening on")
                        .trim()
                        .to_string();
                }
                Ok(_) => {}
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
            assert!(
                Instant::now() < deadline,
                "node did not report listening in time"
            );
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn connect(addr: &str) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(s) = TcpStream::connect(addr) {
            s.set_read_timeout(Some(Duration::from_secs(20))).ok();
            return s;
        }
        assert!(Instant::now() < deadline, "could not connect to {addr}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

struct Client {
    writer: BufWriter<TcpStream>,
    reader: BufReader<TcpStream>,
    next_id: u64,
}

impl Client {
    fn new(addr: &str) -> Client {
        let stream = connect(addr);
        let writer = BufWriter::new(stream.try_clone().unwrap());
        let reader = BufReader::new(stream);
        Client {
            writer,
            reader,
            next_id: 1,
        }
    }

    fn put(&mut self, key: &str, value: &[u8]) -> bool {
        let id = self.next_id;
        self.next_id += 1;
        send_frame(
            &mut self.writer,
            &Frame::PutReq {
                id,
                key: key.to_string(),
                value: value.to_vec(),
            },
        )
        .unwrap();
        match recv_frame(&mut self.reader).unwrap() {
            Frame::PutOk { id: rid } => {
                assert_eq!(rid, id);
                true
            }
            Frame::PutErr { id: rid, msg } => {
                assert_eq!(rid, id);
                panic!("put failed: {msg}");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        let id = self.next_id;
        self.next_id += 1;
        send_frame(
            &mut self.writer,
            &Frame::GetReq {
                id,
                key: key.to_string(),
            },
        )
        .unwrap();
        match recv_frame(&mut self.reader).unwrap() {
            Frame::GetOk { id: rid, value } => {
                assert_eq!(rid, id);
                value
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    fn status(&mut self) -> (u64, Vec<(u64, u64)>) {
        send_frame(&mut self.writer, &Frame::StatusReq).unwrap();
        match recv_frame(&mut self.reader).unwrap() {
            Frame::StatusOk {
                commit_index,
                matches,
            } => (commit_index, matches),
            other => panic!("unexpected response: {other:?}"),
        }
    }
}

fn pattern(i: u64) -> Vec<u8> {
    (0..(64 + (i % 128) as usize))
        .map(|j| ((i * 31 + j as u64 * 7) & 0xFF) as u8)
        .collect()
}

#[test]
fn replication_survives_follower_kill_and_catches_up() {
    let dirs = [
        temp_dir("rep-leader"),
        temp_dir("rep-f1"),
        temp_dir("rep-f2"),
    ];
    let placeholder = "127.0.0.1:0".to_string();

    // Followers first: OS-assigned ports, read back from stdout.
    let mut f1 = ChildGuard::spawn(
        1,
        &[
            placeholder.clone(),
            placeholder.clone(),
            placeholder.clone(),
        ],
        "127.0.0.1:0",
        &dirs[1],
        "follower",
    );
    let mut f2 = ChildGuard::spawn(
        2,
        &[
            placeholder.clone(),
            placeholder.clone(),
            placeholder.clone(),
        ],
        "127.0.0.1:0",
        &dirs[2],
        "follower",
    );
    let f1_addr = f1.wait_listening();
    let f2_addr = f2.wait_listening();

    // Leader dials the real follower addresses.
    let peers = vec!["127.0.0.1:0".to_string(), f1_addr, f2_addr.clone()];
    let mut leader = ChildGuard::spawn(0, &peers, "127.0.0.1:0", &dirs[0], "leader");
    let leader_addr = leader.wait_listening();

    let mut client = Client::new(&leader_addr);

    const PUTS: u64 = 1000;
    for i in 0..PUTS {
        let key = format!("obj/{i:05}");
        let value = pattern(i);
        assert!(client.put(&key, &value), "put {i} must ack");
        if i == 499 {
            f2.kill();
        }
    }

    // Read-your-writes: every ACKed PUT must be visible.
    for i in [0, 250, 999] {
        let key = format!("obj/{i:05}");
        assert_eq!(client.get(&key).unwrap(), pattern(i));
    }

    // Restart the killed follower on the same directory; it must catch up.
    let mut f2 = ChildGuard::spawn(2, &peers, &f2_addr, &dirs[2], "follower");
    f2.wait_listening();

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (commit_index, matches) = client.status();
        let f2_match = matches
            .iter()
            .find(|(id, _)| *id == 2)
            .map(|(_, m)| *m)
            .unwrap_or(0);
        if f2_match >= commit_index {
            break;
        }
        assert!(Instant::now() < deadline, "follower 2 did not catch up");
        std::thread::sleep(Duration::from_millis(100));
    }

    leader.kill();
    f1.kill();
    f2.kill();

    let leader_store = Store::open(&dirs[0]).unwrap();
    let f2_store = Store::open(&dirs[2]).unwrap();
    let committed = leader_store.committed_lsn();
    for key in leader_store.keys() {
        let l = leader_store.get_upto(&key, committed).unwrap();
        let f = f2_store.get_upto(&key, f2_store.committed_lsn()).unwrap();
        assert_eq!(l, f, "key {key} diverged between leader and follower");
    }
}

#[test]
fn put_fails_when_quorum_is_lost() {
    let dirs = [temp_dir("q-leader"), temp_dir("q-f1"), temp_dir("q-f2")];
    let placeholder = "127.0.0.1:0".to_string();

    let mut f1 = ChildGuard::spawn(
        1,
        &[
            placeholder.clone(),
            placeholder.clone(),
            placeholder.clone(),
        ],
        "127.0.0.1:0",
        &dirs[1],
        "follower",
    );
    let mut f2 = ChildGuard::spawn(
        2,
        &[
            placeholder.clone(),
            placeholder.clone(),
            placeholder.clone(),
        ],
        "127.0.0.1:0",
        &dirs[2],
        "follower",
    );
    let f1_addr = f1.wait_listening();
    let f2_addr = f2.wait_listening();

    let peers = vec!["127.0.0.1:0".to_string(), f1_addr, f2_addr.clone()];
    let mut leader = ChildGuard::spawn(0, &peers, "127.0.0.1:0", &dirs[0], "leader");
    let leader_addr = leader.wait_listening();

    let mut client = Client::new(&leader_addr);
    assert!(client.put("warmup", b"warmup-value"));

    f1.kill();
    f2.kill();
    std::thread::sleep(Duration::from_millis(300));

    let id = client.next_id;
    client.next_id += 1;
    send_frame(
        &mut client.writer,
        &Frame::PutReq {
            id,
            key: "doomed".to_string(),
            value: vec![0xAA; 64],
        },
    )
    .unwrap();
    match recv_frame(&mut client.reader).unwrap() {
        Frame::PutErr { id: rid, .. } => assert_eq!(rid, id),
        other => panic!("expected PutErr, got {other:?}"),
    }

    leader.kill();
}
