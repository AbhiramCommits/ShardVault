# ShardVault

Append-only, erasure-coded, replicated object store. Objects land in an
append-only segment store fronted by a write-ahead log (4 KiB checksummed
blocks from a C11 block layer with software CRC-32C), are replicated
across nodes via a single-leader log with majority fsync ACKs, are
erasure-coded with a hand-rolled Reed-Solomon codec over GF(2^8), and are
garbage-collected by crash-safe background compaction. Reads are served
from lock-free committed snapshots, so readers never block on writes. A
fault-injection harness crashes the node at every fsync boundary and
proves the recovery invariants hold.

## Architecture

```
         ┌─────────┐   ┌──────┐   ┌──────────┐   ┌────────────┐   ┌─────────┐
client ─►│ leader  │──►│ WAL  │──►│ segments │──►│ EC stripes │──►│ nodes   │
         └─────────┘   └──────┘   └──────────┘   └────────────┘   └─────────┘
```

## Quickstart

```sh
cargo build --release -p shardvault-node

# Start a 3-node cluster on localhost (leader id 0, followers id 1/2).
mkdir -p /tmp/sv/{n0,n1,n2}
P=127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003
target/release/shardvault-node --id 1 --peers $P --addr 127.0.0.1:17002 --dir /tmp/sv/n1 --role follower &
target/release/shardvault-node --id 2 --peers $P --addr 127.0.0.1:17003 --dir /tmp/sv/n2 --role follower &
target/release/shardvault-node --id 0 --peers $P --addr 127.0.0.1:17001 --dir /tmp/sv/n0 --role leader &

# Put and get an object, list and query aggregates (svctl).
target/release/svctl --addr 127.0.0.1:17001 put hello world
target/release/svctl --addr 127.0.0.1:17001 get hello
target/release/svctl --addr 127.0.0.1:17001 ls ns
target/release/svctl --addr 127.0.0.1:17001 capacity ns
target/release/svctl --addr 127.0.0.1:17001 node-status

# Simulate a follower failure on the leader, then put and read back: the
# leader still ACKs (leader + follower 1 = majority) and serves the object.
target/release/svctl --addr 127.0.0.1:17001 simulate-failure 2
target/release/svctl --addr 127.0.0.1:17001 put after-crash still-here
target/release/svctl --addr 127.0.0.1:17001 get after-crash
```

## Durability contract

Every fsync follows a strict order; this is the whole contract:

1. The value bytes are written to the segment file.
2. The segment file is fsynced.
3. The `Commit` WAL record is appended.
4. The WAL is fsynced.

A client PUT is **ACKed only after all four steps completed**, so an
ACKed PUT survives any crash: recovery replays the WAL (which contains the
commit) and the data it points at was fsynced before the commit was
durable. If the process dies between steps 2 and 4, the commit record is
not durable, recovery discards the uncommitted records, and the segment
tail is truncated — the PUT was never ACKed, so losing it is correct.
Replication adds one more step before step 3: the leader fsyncs its WAL
and waits for a majority of follower fsync ACKs, so a commit only exists
when a quorum has the records durably.

## Erasure coding

`shardvault-ec` is a from-scratch Reed-Solomon codec over GF(2^8)
(polynomial 0x11D), systematic Vandermonde encoding matrix, no external
coding crates. Any (k, m) with k+m <= 255 is supported (tested: (4,2),
(6,3), (10,4)); any m shard failures are tolerated per stripe, and reads
reconstruct transparently from any k survivors. Measured encode
throughput (criterion, (10,4), ~1 MiB stripes, this machine):

    ~355 MiB/s (2.8 ms per 1 MiB stripe)

## Compaction

Sealed segments are garbage-collected in the background: a segment with
dead (overwritten) values is rewritten keeping only live values plus dead
values newer than a safety horizon (the minimum of every follower's
committed point, so lagging followers can always be backfilled). The
replacement is written to a temp file, fsynced, renamed into place, and
described by committed `SegmentSwap`/`SegmentRemap` WAL records — a crash
at any point leaves either the old or the new segment fully valid, never
both partially. `svctl` triggers a pass; the node's `Compact` frame does
the same over the wire, and followers rebuild replacements from their own
segment copies. The crash matrix exercises compaction fsync boundaries
alongside PUT fsyncs.

## Concurrency

The store has one writer (all mutating methods hold an internal `RwLock`)
and lock-free readers: reads load an `arc-swap` snapshot containing the
committed index, open file handles, and aggregates, so a reader never
takes a lock and never blocks on fsyncs or compaction. `StoreView` is the
shareable read handle. An 8-reader/1-writer stress test
(`crates/shardvault-core/tests/concurrency.rs`) asserts readers only ever
observe fully written values, and CI runs it under ThreadSanitizer.

## Rolled-up metadata

`capacity(prefix)` answers from per-prefix counters updated in the same
WAL transaction as each PUT, so it costs one hash-map lookup — O(depth)
in the prefix length, never O(objects). Measured with 100,000 objects
(`cargo run --release -p shardvault-core --example capacity_bench`):

    ~13 ns per capacity("") lookup at 100k objects (median of 3 runs)

Recovery rebuilds the counters from the committed WAL records, and the
100k-object test asserts `capacity("")` matches a brute-force recount in
under 1 ms.

## Replication

Three-node cluster on localhost, one client, sequential PUTs; measured by
`python3 harness/bench_latency.py` (full table in
[reports/latency.md](reports/latency.md)):

| scenario | mean | p50 | p99 |
| --- | --- | --- | --- |
| 3 nodes healthy | 22.17 ms | 22.18 ms | 31.88 ms |
| 1 follower killed | 16.34 ms | 16.25 ms | 23.65 ms |

With one follower killed the leader still commits on the remaining
majority; if a majority is unreachable, PUTs return an error instead of
ACKing an uncommitted write. A restarted follower backfills from its last
durable commit and converges to the leader byte-for-byte.

## Crash recovery

The harness (`SV_FAIL_AT_FSYNC=n` aborts the node on its nth fsync)
enumerates **63 fsync boundaries** (20-put workload with overwrites and
compaction) and at each one asserts: the store reopens, every ACKed value
survives (or is superseded by a later committed version), no torn values
exist, and aggregates equal a brute-force recount of distinct objects.
All 63 pass — see [reports/crash_matrix.md](reports/crash_matrix.md).

## Testing

| layer | what it covers | where |
| --- | --- | --- |
| C unit | CRC-32C vectors, block encode/decode, corruption | `csrc/test_block.c` |
| Rust unit | GF(2^8) tables, matrix invertibility, rollup prefixes | `src/*` `#[cfg(test)]` |
| proptest | GF laws (commutativity, distributivity, inverses) | `crates/shardvault-ec/tests/gf_props.rs` |
| EC erasure matrix | every erasure combination up to m, m+1 fails cleanly | `crates/shardvault-ec/tests/rs.rs` |
| store integration | WAL/store roundtrip, recovery, torn blocks, stripes | `crates/shardvault-core/tests/` |
| replication integration | 3-node cluster, kill/restart/catch-up, quorum loss, compaction replication | `crates/shardvault-node/tests/replication.rs` |
| compaction | reclaim, crash-before/after commit, horizon safety | `crates/shardvault-core/tests/compaction.rs` |
| concurrency | 8 readers + 1 writer, no torn observations; TSan in CI | `crates/shardvault-core/tests/concurrency.rs` |
| svctl | put/get/ls/capacity/node-status/simulate-failure smoke-tested | `crates/shardvault-node/src/bin/svctl.rs` |
| crash matrix | every fsync boundary, 3 seeds in CI | `harness/test_crash_recovery.py` |
| sanitizers | C block layer under ASan+UBSan | CI job `c` |
| miri | pure-Rust GF/matrix/rollup under miri | CI job `miri` |

## Layout

- `crates/shardvault-core/` — segment store, WAL, metadata, EC striping; C block layer FFI
- `crates/shardvault-ec/` — Reed-Solomon erasure coding over GF(2^8)
- `crates/shardvault-node/` — single-node TCP daemon, single-leader replication
- `csrc/` — C11 block layer (on-disk 4 KiB block format, software CRC-32C)
- `harness/` — Python fault-injection harness, latency bench, client CLI
- `reports/` — crash matrix and latency measurements (committed)

## Building and testing

- `cargo test --workspace` — all Rust tests
- `make test-c` / `make -C csrc test-san` — C tests / sanitizer build
- `cargo bench -p shardvault-ec` — EC encode throughput
- `python3 -m pytest harness/ -q --seed N` — crash matrix (build the node
  with `--features fault-injection` first)
