use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use shardvault_core::ffi::block;
use shardvault_core::segment::Store;
use shardvault_node::node::{run, Config};
use shardvault_node::protocol::{ROLE_FOLLOWER, ROLE_LEADER};

fn usage() -> ! {
    eprintln!(
        "usage: shardvault-node --id N --peers addr[,addr...] [--addr ADDR] --dir PATH \
         --role leader|follower\n       shardvault-node --verify --dir PATH"
    );
    std::process::exit(2);
}

fn verify(dir: &std::path::Path) -> i32 {
    match Store::open(dir) {
        Ok(store) => {
            let committed = store.committed_lsn();
            let mut keys = Vec::new();
            for (key, len) in store.committed_keys() {
                match store.get_upto(&key, committed) {
                    Ok(Some(value)) => {
                        keys.push(serde_json::json!({
                            "key": key,
                            "len": len,
                            "crc": format!("{:08x}", block::crc32c(&value)),
                        }));
                    }
                    Ok(None) => {
                        eprintln!("verify: committed key vanished: {key}");
                        return 1;
                    }
                    Err(e) => {
                        eprintln!("verify: read failed for {key}: {e}");
                        return 1;
                    }
                }
            }
            let agg = store.capacity("");
            let report = serde_json::json!({
                "ok": true,
                "committed_lsn": committed,
                "keys": keys,
                "aggregate": {
                    "object_count": agg.object_count,
                    "byte_count": agg.byte_count,
                },
            });
            println!("{report}");
            0
        }
        Err(e) => {
            eprintln!("verify failed: {e}");
            1
        }
    }
}

fn main() -> ExitCode {
    let mut verify_mode = false;
    let mut id: Option<u64> = None;
    let mut peers: Option<Vec<String>> = None;
    let mut addr: Option<String> = None;
    let mut dir: Option<PathBuf> = None;
    let mut role: Option<u8> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--verify" => verify_mode = true,
            "--id" => match args.next() {
                Some(v) => id = Some(v.parse().unwrap_or_else(|_| usage())),
                None => usage(),
            },
            "--peers" => match args.next() {
                Some(v) => peers = Some(v.split(',').map(|s| s.trim().to_string()).collect()),
                None => usage(),
            },
            "--addr" => match args.next() {
                Some(v) => addr = Some(v.to_string()),
                None => usage(),
            },
            "--dir" => match args.next() {
                Some(v) => dir = Some(PathBuf::from(v)),
                None => usage(),
            },
            "--role" => match args.next().as_deref() {
                Some("leader") => role = Some(ROLE_LEADER),
                Some("follower") => role = Some(ROLE_FOLLOWER),
                _ => usage(),
            },
            _ => usage(),
        }
    }

    if verify_mode {
        let dir = dir.unwrap_or_else(|| usage());
        return ExitCode::from(verify(&dir) as u8);
    }

    let id = id.unwrap_or_else(|| usage());
    let peers = peers.unwrap_or_else(|| usage());
    let dir = dir.unwrap_or_else(|| usage());
    let role = role.unwrap_or_else(|| usage());
    let addr = addr.unwrap_or_else(|| peers.get(id as usize).cloned().unwrap_or_else(|| usage()));

    match run(Config {
        id,
        addr,
        peers,
        dir,
        role,
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("shardvault-node: {e}");
            ExitCode::from(1)
        }
    }
}
