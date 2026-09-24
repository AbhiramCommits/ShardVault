use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use shardvault_ec::rs::ReedSolomon;

fn bench_encode(c: &mut Criterion) {
    let k = 10;
    let m = 4;
    let rs = ReedSolomon::new(k, m);
    let shard_len = 104_857;
    let data: Vec<Vec<u8>> = (0..k).map(|i| vec![i as u8; shard_len]).collect();
    let total = (k * shard_len) as u64;
    let mut group = c.benchmark_group("encode");
    group.throughput(Throughput::Bytes(total));
    group.bench_function("rs-10-4-1MiB", |b| b.iter(|| rs.encode(&data)));
    group.finish();
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);
