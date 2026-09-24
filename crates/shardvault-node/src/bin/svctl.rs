//! ShardVault control client: put/get/ls/capacity/node-status and
//! leader-side failure simulation.

use clap::{Parser, Subcommand};
use shardvault_node::protocol::{recv_frame, send_frame, Frame};
use std::io::{BufReader, BufWriter};
use std::net::TcpStream;

#[derive(Parser)]
#[command(name = "svctl", about = "ShardVault control client")]
struct Cli {
    /// Leader address (HOST:PORT)
    #[arg(long)]
    addr: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Put an object
    Put { key: String, value: String },
    /// Get an object
    Get { key: String },
    /// List objects under a prefix
    Ls { prefix: String },
    /// Rolled-up capacity vs brute-force recount, side by side
    Capacity { prefix: String },
    /// Leader status: commit index and per-follower match indexes
    NodeStatus,
    /// Mark a follower as failed on the leader (until the leader restarts)
    SimulateFailure { node_id: u64 },
}

struct Client {
    writer: BufWriter<TcpStream>,
    reader: BufReader<TcpStream>,
    next_id: u64,
}

impl Client {
    fn connect(addr: &str) -> Client {
        let stream = TcpStream::connect(addr).unwrap_or_else(|e| {
            eprintln!("svctl: cannot connect to {addr}: {e}");
            std::process::exit(1);
        });
        Client {
            writer: BufWriter::new(stream.try_clone().unwrap()),
            reader: BufReader::new(stream),
            next_id: 1,
        }
    }

    fn req(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn put(&mut self, key: &str, value: &[u8]) {
        let id = self.req();
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
                println!("ack");
            }
            Frame::PutErr { id: rid, msg } => {
                assert_eq!(rid, id);
                eprintln!("svctl: put rejected: {msg}");
                std::process::exit(1);
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }

    fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        let id = self.req();
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
            other => panic!("unexpected frame {other:?}"),
        }
    }

    fn list(&mut self, prefix: &str) -> Vec<(String, u32)> {
        let id = self.req();
        send_frame(
            &mut self.writer,
            &Frame::ListReq {
                id,
                prefix: prefix.to_string(),
            },
        )
        .unwrap();
        match recv_frame(&mut self.reader).unwrap() {
            Frame::ListOk { id: rid, keys } => {
                assert_eq!(rid, id);
                keys
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }

    fn capacity(&mut self, prefix: &str) -> ((u64, u64), (u64, u64)) {
        let id = self.req();
        send_frame(
            &mut self.writer,
            &Frame::CapacityReq {
                id,
                prefix: prefix.to_string(),
            },
        )
        .unwrap();
        match recv_frame(&mut self.reader).unwrap() {
            Frame::CapacityOk {
                id: rid,
                aggregate,
                brute,
            } => {
                assert_eq!(rid, id);
                (aggregate, brute)
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }

    fn status(&mut self) -> (u64, Vec<(u64, u64)>) {
        send_frame(&mut self.writer, &Frame::StatusReq).unwrap();
        match recv_frame(&mut self.reader).unwrap() {
            Frame::StatusOk {
                commit_index,
                matches,
            } => (commit_index, matches),
            other => panic!("unexpected frame {other:?}"),
        }
    }

    fn simulate_failure(&mut self, node_id: u64) {
        send_frame(&mut self.writer, &Frame::SimulateFailure { node_id }).unwrap();
        match recv_frame(&mut self.reader).unwrap() {
            Frame::SimulateFailureDone => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }
}

fn main() {
    let cli = Cli::parse();
    let mut client = Client::connect(&cli.addr);
    match cli.cmd {
        Cmd::Put { key, value } => client.put(&key, value.as_bytes()),
        Cmd::Get { key } => match client.get(&key) {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None => println!("(none)"),
        },
        Cmd::Ls { prefix } => {
            for (key, len) in client.list(&prefix) {
                println!("{key}\t{len}");
            }
        }
        Cmd::Capacity { prefix } => {
            let ((agg_c, agg_b), (brute_c, brute_b)) = client.capacity(&prefix);
            println!("prefix    objects bytes");
            println!("rollup    {agg_c:>7} {agg_b:>8}");
            println!("recount   {brute_c:>7} {brute_b:>8}");
            if (agg_c, agg_b) == (brute_c, brute_b) {
                println!("OK: rollup matches brute-force recount");
            } else {
                println!("MISMATCH: rollup disagrees with recount");
            }
        }
        Cmd::NodeStatus => {
            let (commit_index, matches) = client.status();
            println!("commit_index: {commit_index}");
            for (id, m) in matches {
                println!("  follower {id}: match_index {m}");
            }
        }
        Cmd::SimulateFailure { node_id } => {
            client.simulate_failure(node_id);
            println!("marked follower {node_id} as failed on the leader");
        }
    }
}
