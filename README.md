# ShardVault

Append-only, erasure-coded, replicated object store.

## Architecture

```
         ┌─────────┐   ┌──────┐   ┌──────────┐   ┌────────────┐   ┌─────────┐
client ─►│ leader  │──►│ WAL  │──►│ segments │──►│ EC stripes │──►│ nodes   │
         └─────────┘   └──────┘   └──────────┘   └────────────┘   └─────────┘
```

## Layout

- `crates/shardvault-core/` — segment store, WAL, metadata; C block layer FFI
- `crates/shardvault-ec/` — Reed-Solomon erasure coding over GF(2^8)
- `crates/shardvault-node/` — single-node TCP daemon
- `csrc/` — C11 block layer (on-disk 4 KiB block format, software CRC-32C)
- `harness/` — Python fault-injection harness (pytest)

## On-disk block format

Each block is 4096 bytes: a 24-byte little-endian header followed by up to
4072 bytes of payload. The header is `u64 lsn`, `u32 payload_len`,
`u32 crc32c`, `u8 flags`, `u8 pad[3]` plus 4 bytes of tail padding to reach
24 bytes. The CRC-32C (Castagnoli, poly 0x82F63B78) covers the header with
the CRC field zeroed plus the payload.

## Erasure coding

`shardvault-ec` is a hand-rolled Reed-Solomon codec over GF(2^8) (poly
0x11D), systematic Vandermonde encoding matrix, no external coding crates.
`shardvault-core` spreads each stripe over simulated node directories as
C-encoded, CRC-checksummed blocks and reconstructs transparently on read.

Benchmark (`cargo bench -p shardvault-ec`, (10,4), ~1 MiB stripes):

    encode/rs-10-4-1MiB    ~296 MiB/s

## Replication

`shardvault-node` runs as a leader or follower over TCP (length-prefixed
bincode frames, `serde`). PUTs flow: leader stages + fsyncs its WAL, ships
records to followers, waits for a majority of fsync ACKs, commits, and only
then replies. Followers apply records at the leader's LSNs; lagging
followers backfill from their `NeedFrom` LSN. Reads are served only up to
the commit index (read-your-writes), and a PUT never ACKs without a
durable quorum commit.

```sh
shardvault-node --id 0 --peers A0,A1,A2 --addr A0 --dir /data/leader --role leader
shardvault-node --id 1 --peers A0,A1,A2 --addr A1 --dir /data/f1 --role follower
```

## Fault injection

With the `fault-injection` cargo feature (enabled in `shardvault-node`),
`SV_FAIL_AT_FSYNC=n` makes the node hard-abort on its nth fsync. The
Python harness (`harness/`) drives a workload, crashes the node at every
fsync boundary, and asserts the recovery invariants:

```sh
cargo build -p shardvault-node
python3 -m pytest harness/test_crash_recovery.py --seed 12345
python3 harness/report.py --seed 12345   # writes reports/crash_matrix.md
```

See `reports/crash_matrix.md` for the latest run (60 boundaries, 0 failures).

## Building and testing

- `make test-c` — C unit tests (`csrc/test_block.c`)
- `cargo test` — Rust tests (FFI roundtrip, corruption detection, CRC vectors)
- `make test` — both
