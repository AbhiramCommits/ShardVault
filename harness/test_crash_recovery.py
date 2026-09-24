"""Crash-recovery matrix: enumerate every fsync boundary and assert the
recovery invariants after aborting the node at that boundary.

Run:  python3 -m pytest harness/test_crash_recovery.py --seed N
"""

import functools
import os
import random
import shutil
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import crash  # noqa: E402

WORKDIR = tempfile.mkdtemp(prefix="sv-matrix-")
SEED = None


@functools.lru_cache(maxsize=1)
def matrix(seed):
    """Probe a healthy run once to learn the total fsync count M."""
    workdir = tempfile.mkdtemp(prefix="sv-probe-")
    try:
        return crash.probe_total_fsyncs(
            seed, crash.gen_workload(seed, crash.N_PUTS), workdir
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def resolve_seed(config):
    seed = config.getoption("--seed") or int(os.environ.get("SV_SEED", "0"))
    if seed == 0:
        seed = random.randrange(1 << 32)
    return seed


def pytest_generate_tests(metafunc):
    global SEED
    if "boundary" in metafunc.fixturenames:
        SEED = resolve_seed(metafunc.config)
        print("harness: crash matrix seed=%d puts=%d" % (SEED, crash.N_PUTS), flush=True)
        m = matrix(SEED)
        metafunc.parametrize("boundary", list(range(1, m + 1)))


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(WORKDIR, ignore_errors=True)


def test_crash_recovery_invariants(boundary):
    workload = crash.gen_workload(SEED, crash.N_PUTS)
    acked, report = crash.run_boundary(SEED, workload, boundary, WORKDIR)
    violations = crash.check_invariants(workload, acked, report)
    assert violations == [], "boundary %d: %s" % (boundary, "; ".join(violations))
