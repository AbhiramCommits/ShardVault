# ShardVault design

## Durability and consistency invariants

Each invariant names the test that proves it (file, test name).

1. **Block integrity.** Any bit flip in a 4 KiB block (header or payload)
   is detected by the CRC-32C before the block is ever used.
   Proved by: `csrc/test_block.c` (corruption cases) and
   `crates/shardvault-core/tests/block.rs::payload_corruption_returns_crc_error`,
   `...::header_corruption_returns_crc_error`.

2. **WAL replay never reads past corruption.** Recovery stops at the first
   torn (short) or CRC-failing block and ignores everything after it.
   Proved by: `crates/shardvault-core/tests/wal.rs::replay_stops_at_torn_block`,
   `...::replay_stops_at_crc_failure`.

3. **Only committed records survive recovery.** Records after the last
   durable `Commit` are discarded, and their segment bytes are truncated
   away; LSNs are never reused.
   Proved by: `crates/shardvault-core/tests/wal.rs::records_after_last_commit_are_discarded`,
   `crates/shardvault-core/tests/segment.rs::uncommitted_tail_is_discarded_after_crash`.

4. **ACK implies durable.** A PUT is acknowledged only after segment
   fsync, `Commit` record, and WAL fsync have all completed, so an ACKed
   PUT is always present after a crash at any fsync boundary.
   Proved by: `harness/test_crash_recovery.py` (acked-keys invariant at
   every boundary) and `crates/shardvault-node/tests/replication.rs::replication_survives_follower_kill_and_catches_up`
   (GET after ACKed PUT).

5. **Quorum safety.** The leader never commits, and therefore never ACKs,
   without a durable majority; if a majority is unreachable the PUT
   errors.
   Proved by: `crates/shardvault-node/tests/replication.rs::put_fails_when_quorum_is_lost`.

6. **Read-your-writes.** Reads are served only up to the commit index, so
   a GET after an ACKed PUT always observes the new value.
   Proved by: `crates/shardvault-node/tests/replication.rs::replication_survives_follower_kill_and_catches_up`
   (GET checks after ACKs).

7. **Follower convergence.** A follower that is killed and restarted
   backfills from its last durable commit block and ends up byte-identical
   to the leader for every key.
   Proved by: `crates/shardvault-node/tests/replication.rs::replication_survives_follower_kill_and_catches_up`
   (post-restart store comparison).

8. **Erasure coding exactness.** With (k, m), any subset of up to m
   missing shards reconstructs byte-for-byte; m+1 missing shards fail with
   `TooFewShards` and never produce a wrong answer.
   Proved by: `crates/shardvault-ec/tests/rs.rs::reconstruction_all_erasure_combinations`
   (all erasure masks for (4,2), (6,3), (10,4)).

9. **Encoding matrix invertibility.** Every k-row submatrix of the
   systematic encoding matrix is invertible, which is what makes
   invariant 8 hold for every survivor set.
   Proved by: `crates/shardvault-ec/src/matrix.rs::every_k_row_submatrix_is_invertible`.

10. **GF(2^8) laws.** Multiplication is commutative/associative and
    distributes over addition; nonzero elements have inverses.
    Proved by: `crates/shardvault-ec/tests/gf_props.rs` (proptest).

11. **Stripe transparency.** A stripe spread over k+m node directories
    reads back intact with up to m shard files missing or CRC-corrupt.
    Proved by: `crates/shardvault-core/tests/stripe.rs::two_erasures_recover_with_4_2`,
    `...::three_erasures_recover_with_4_3`,
    `...::three_erasures_fail_with_4_2`.

12. **Aggregate exactness.** Rolled-up prefix aggregates always equal a
    brute-force recount of the surviving objects, both live and after
    crash recovery.
    Proved by: `crates/shardvault-core/tests/aggregates.rs::aggregates_match_bruteforce_after_100k_puts`
    and `harness/test_crash_recovery.py` (aggregate invariant).

## Known limitations

- **No leader election.** The leader is fixed at process startup; a
  crashed leader requires manual failover. There is no term/epoch
  mechanism, so two nodes started as leader for the same membership are
  undefined behavior.
- **Simulated nodes share one host.** "Nodes" are processes/directories
  on a single machine; a host failure would take the whole cluster down,
  and the integration tests exercise process crashes, not network
  partitions or disk failure.
- **No compaction.** Segments and WAL grow forever; no garbage
  collection of overwritten object versions exists yet.
- **WAL amplification.** One record per 4 KiB block means ~8 KiB of WAL
  per PUT (two records); fine for correctness, wasteful for throughput.
- **Sequential leader.** PUTs are processed one at a time on the leader;
  latency numbers in `reports/latency.md` reflect that.
- **Followers do not serve reads.** GETs are leader-only.
- **Miri does not cover the FFI.** The C block layer runs under
  ASan/UBSan instead; miri checks the pure-Rust modules (gf, matrix,
  rollup).
