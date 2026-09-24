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

## Building and testing

- `make test-c` — C unit tests (`csrc/test_block.c`)
- `cargo test` — Rust tests (FFI roundtrip, corruption detection, CRC vectors)
- `make test` — both
