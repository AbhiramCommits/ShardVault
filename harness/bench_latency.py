#!/usr/bin/env python3
"""Measures sequential PUT latency (p50/p99) for a 3-node cluster:
(a) all nodes healthy, (b) one follower killed. Writes reports/latency.md."""

import os
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import crash  # noqa: E402

REPO = Path(__file__).resolve().parent.parent

WARMUP = 50
MEASURED = int(os.environ.get("SV_LATENCY_PUTS", "500"))


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    idx = min(len(s) - 1, int(p * len(s)))
    return s[idx]


def measure(client, workload, n):
    latencies = []
    for i in range(n):
        key, value = workload[i % len(workload)]
        start = time.monotonic()
        ok = client.put(key, value)
        elapsed = time.monotonic() - start
        if not ok:
            raise RuntimeError("put not acked")
        latencies.append(elapsed)
    return latencies


def main():
    workdir = Path(tempfile.mkdtemp(prefix="sv-latency-"))
    n_puts = WARMUP + MEASURED
    workload = crash.gen_workload(0xC0FFEE, n_puts)

    dirs = [workdir / "n0", workdir / "n1", workdir / "n2"]
    for d in dirs:
        d.mkdir(parents=True)

    f1, f1_addr = crash.spawn_node(1, ["127.0.0.1:0"] * 3, "127.0.0.1:0", dirs[1], "follower")
    f2, f2_addr = crash.spawn_node(2, ["127.0.0.1:0"] * 3, "127.0.0.1:0", dirs[2], "follower")
    leader, leader_addr = crash.spawn_node(
        0, ["127.0.0.1:0", f1_addr, f2_addr], "127.0.0.1:0", dirs[0], "leader"
    )

    client = crash.NodeClient(leader_addr)
    for i in range(WARMUP):
        key, value = workload[i]
        assert client.put(key, value)

    healthy = measure(client, workload[WARMUP:], MEASURED)

    f2.kill()
    f2.wait()
    time.sleep(0.5)
    for i in range(10):
        key, value = workload[i]
        assert client.put(key, value)
    degraded = measure(client, workload, MEASURED)

    crash.stop_node(leader)
    crash.stop_node(f1)

    def row(label, latencies):
        return (
            "| %s | %.2f | %.2f | %.2f | %.2f |"
            % (
                label,
                statistics.mean(latencies) * 1000,
                percentile(latencies, 0.5) * 1000,
                percentile(latencies, 0.99) * 1000,
                max(latencies) * 1000,
            )
        )

    lines = [
        "# PUT latency",
        "",
        "- cluster: 3 nodes on 127.0.0.1, one client, sequential PUTs",
        "- measured PUTs per scenario: %d (plus warmup)" % MEASURED,
        "- generated: %s" % datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "",
        "| scenario | mean (ms) | p50 (ms) | p99 (ms) | max (ms) |",
        "| --- | --- | --- | --- | --- |",
        row("3 nodes healthy", healthy),
        row("1 follower killed", degraded),
        "",
    ]
    out = REPO / "reports" / "latency.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    print("\n".join(lines))
    print("wrote %s" % out)

    import shutil

    shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
