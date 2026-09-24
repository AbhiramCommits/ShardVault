#!/usr/bin/env python3
"""Runs the full fsync crash matrix and emits reports/crash_matrix.md."""

import argparse
import os
import random
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import crash  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="ShardVault crash matrix report")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--puts", type=int, default=crash.N_PUTS)
    parser.add_argument("--out", type=str, default=None)
    args = parser.parse_args()

    seed = args.seed or int(os.environ.get("SV_SEED", "0"))
    if seed == 0:
        seed = random.randrange(1 << 32)
    print("harness: crash matrix seed=%d puts=%d" % (seed, args.puts))

    workdir = Path(tempfile.mkdtemp(prefix="sv-report-"))
    started = time.time()
    try:
        seed, m, results = crash.run_matrix(seed, args.puts, workdir)
    finally:
        import shutil

        shutil.rmtree(workdir, ignore_errors=True)
    elapsed = time.time() - started

    failed = [r for r in results if r["violations"]]
    lines = [
        "# Crash recovery matrix",
        "",
        "- seed: %d" % seed,
        "- workload: %d puts" % args.puts,
        "- fsync boundaries enumerated: %d" % m,
        "- failures: %d" % len(failed),
        "- generated: %s" % datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "- matrix duration: %.1fs" % elapsed,
        "",
        "| boundary | outcome | recovered objects | aggregate delta |",
        "| --- | --- | --- | --- |",
    ]
    for r in results:
        outcome = "ok" if not r["violations"] else "; ".join(r["violations"])[:80]
        lines.append(
            "| %d | %s | %d | %d |"
            % (r["boundary"], outcome, r["recovered"], r["delta"])
        )
    lines.append("")

    out = Path(args.out) if args.out else Path(__file__).resolve().parent.parent / "reports" / "crash_matrix.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    print("wrote %s (%d boundaries, %d failures)" % (out, m, len(failed)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
