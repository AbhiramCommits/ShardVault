//! Length-prefixed bincode frames exchanged between clients and nodes, and
//! between the leader and its followers.
//!
//! Wire format: `u32` little-endian payload length, followed by a bincode
//! (little-endian, fixed-int) encoding of the [`Frame`] enum. The enum
//! variant order below defines the u32 variant tags used on the wire; the
//! Python harness mirrors these tags by hand.

use serde::{Deserialize, Serialize};
use shardvault_core::wal::Record;
use std::io::{self, Read, Write};

pub const ROLE_LEADER: u8 = 0;
pub const ROLE_FOLLOWER: u8 = 1;

const MAX_FRAME: usize = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Frame {
    Hello {
        id: u64,
        role: u8,
    }, // 0
    PutReq {
        id: u64,
        key: String,
        value: Vec<u8>,
    }, // 1
    PutOk {
        id: u64,
    }, // 2
    PutErr {
        id: u64,
        msg: String,
    }, // 3
    GetReq {
        id: u64,
        key: String,
    }, // 4
    GetOk {
        id: u64,
        value: Option<Vec<u8>>,
    }, // 5
    Append {
        lsn: u64,
        rec: Record,
        data: Option<Vec<u8>>,
    }, // 6
    SyncBarrier {
        lsn: u64,
    }, // 7
    SyncAck {
        lsn: u64,
    }, // 8
    NeedFrom {
        lsn: u64,
    }, // 9
    StatusReq, // 10
    StatusOk {
        commit_index: u64,
        matches: Vec<(u64, u64)>,
    }, // 11
    ProbeFsync, // 12
    ProbeFsyncResp {
        count: u64,
    }, // 13
    Compact,   // 14
    CompactDone {
        freed: u64,
    }, // 15
    ListReq {
        id: u64,
        prefix: String,
    }, // 16
    ListOk {
        id: u64,
        keys: Vec<(String, u32)>,
    }, // 17
    CapacityReq {
        id: u64,
        prefix: String,
    }, // 18
    CapacityOk {
        id: u64,
        aggregate: (u64, u64),
        brute: (u64, u64),
    }, // 19
    SimulateFailure {
        node_id: u64,
    }, // 20
    SimulateFailureDone, // 21
}

fn bad_data(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

pub fn send_frame<W: Write>(w: &mut W, frame: &Frame) -> io::Result<()> {
    let body = bincode::serialize(frame).map_err(bad_data)?;
    if body.len() > MAX_FRAME {
        return Err(bad_data("frame too large"));
    }
    w.write_all(&(body.len() as u32).to_le_bytes())?;
    w.write_all(&body)?;
    w.flush()
}

pub fn recv_frame<R: Read>(r: &mut R) -> io::Result<Frame> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let n = u32::from_le_bytes(len) as usize;
    if n > MAX_FRAME {
        return Err(bad_data("frame too large"));
    }
    let mut body = vec![0u8; n];
    r.read_exact(&mut body)?;
    bincode::deserialize(&body).map_err(bad_data)
}
