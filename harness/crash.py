"""Fault-injection runner for the ShardVault node.

Drives a single leader node over TCP (bincode frames), kills it at a
configurable fsync boundary via SV_FAIL_AT_FSYNC, and verifies the store
with the node's --verify mode.

The bincode wire format used here mirrors `shardvault-node/src/protocol.rs`
(bincode 1.3 defaults: little-endian fixed ints, u64 string lengths, u32
enum variant tags). Compatible with Python 3.9+.
"""

import json
import os
import random
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
NODE_BIN = Path(os.environ.get("SV_NODE_BIN", REPO / "target" / "debug" / "shardvault-node"))

N_PUTS = int(os.environ.get("SV_N_PUTS", "20"))
PUT_TIMEOUT = float(os.environ.get("SV_PUT_TIMEOUT", "3"))


# --- CRC-32C (Castagnoli, poly 0x82F63B78, reflected) -------------------

_POLY = 0x82F63B78


def _crc_table():
    table = []
    for b in range(256):
        r = b
        for _ in range(8):
            r = (r >> 1) ^ (_POLY & (0xFFFFFFFF & -(r & 1)))
        table.append(r)
    return table


_CRC_TABLE = _crc_table()


def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return (~crc) & 0xFFFFFFFF


# --- bincode helpers -----------------------------------------------------


def _u32(v):
    return struct.pack("<I", v)


def _u64(v):
    return struct.pack("<Q", v)


def _string(s):
    b = s.encode("utf-8")
    return _u64(len(b)) + b


def _bytes(b):
    return _u64(len(b)) + b


def _frame(body):
    return _u32(len(body)) + body


def _recv_exact(sock, n):
    out = b""
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise ConnectionError("connection closed")
        out += chunk
    return out


def _recv_frame(sock):
    header = _recv_exact(sock, 4)
    n = struct.unpack("<I", header)[0]
    return _recv_exact(sock, n)


def _parse_u64(buf, off):
    return struct.unpack("<Q", buf[off : off + 8])[0], off + 8


def _parse_string(buf, off):
    n, off = _parse_u64(buf, off)
    return buf[off : off + n].decode("utf-8"), off + n


# Frame variant tags, matching protocol.rs declaration order.
TAG_PUT_OK = 2
TAG_PUT_ERR = 3
TAG_GET_OK = 5
TAG_STATUS_OK = 11
TAG_PROBE_RESP = 13
TAG_COMPACT_DONE = 15


def put_request_body(rid, key, value):
    return _u32(1) + _u64(rid) + _string(key) + _bytes(value)


def get_request_body(rid, key):
    return _u32(4) + _u64(rid) + _string(key)


def probe_request_body():
    return _u32(12)


# --- workload generation -------------------------------------------------


def gen_workload(seed, n):
    """Deterministic workload with ~30% overwrites so sealed segments
    accumulate dead values for compaction."""
    rng = random.Random(seed)
    workload = []
    pool = []
    for i in range(n):
        if i % 4 == 3 and pool:
            key = rng.choice(pool)
        else:
            key = "ns%02d/s%02d/k%06d" % (i % 5, i % 7, i)
            pool.append(key)
        value = bytes(rng.getrandbits(8) for _ in range(rng.randrange(50, 400)))
        workload.append((key, value))
    return workload


# --- node process management ----------------------------------------------


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def spawn_node(id_, peers, addr, datadir, role, fail_at=None):
    """Spawns a node; returns (proc, bound_addr)."""
    env = dict(os.environ)
    env.setdefault("SV_SEGMENT_MAX_BYTES", "2048")
    if fail_at is not None:
        env["SV_FAIL_AT_FSYNC"] = str(fail_at)
    proc = subprocess.Popen(
        [
            str(NODE_BIN),
            "--id", str(id_),
            "--peers", ",".join(peers),
            "--addr", addr,
            "--dir", str(datadir),
            "--role", role,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    bound = None
    deadline = time.time() + 20
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                rest = proc.stdout.read()
                raise RuntimeError(
                    "node exited before listening: %s" % rest.decode(errors="replace")
                )
            time.sleep(0.02)
            continue
        text = line.decode(errors="replace").strip()
        if text.startswith("listening on "):
            bound = text[len("listening on "):]
            break
    if bound is None:
        proc.kill()
        raise RuntimeError("node never reported its listen address")

    def drain():
        while True:
            chunk = proc.stdout.read(4096)
            if not chunk:
                return

    threading.Thread(target=drain, daemon=True).start()
    return proc, bound


def start_node(datadir, fail_at):
    """Spawns a single leader node (harness default); returns (proc, addr)."""
    return spawn_node(0, ["127.0.0.1:0"], "127.0.0.1:0", datadir, "leader", fail_at)


def stop_node(proc):
    if proc.poll() is None:
        proc.kill()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# --- client ---------------------------------------------------------------


class NodeClient:
    def __init__(self, addr):
        self.addr = addr
        self.sock = None
        self.next_id = 1
        self.connect()

    def connect(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = socket.create_connection((self.addr.split(":")[0], int(self.addr.split(":")[1])), timeout=5)
        self.sock.settimeout(PUT_TIMEOUT)

    def put(self, key, value):
        rid = self.next_id
        self.next_id += 1
        try:
            self.sock.sendall(_frame(put_request_body(rid, key, value)))
            body = _recv_frame(self.sock)
        except (socket.timeout, ConnectionError, OSError):
            return False
        tag = struct.unpack("<I", body[:4])[0]
        if tag == TAG_PUT_OK:
            return True
        if tag == TAG_PUT_ERR:
            rid2, off = _parse_u64(body, 4)
            msg, _ = _parse_string(body, off)
            raise RuntimeError("put rejected (rid %d): %s" % (rid2, msg))
        raise RuntimeError("unexpected frame tag %d" % tag)

    def get(self, key):
        rid = self.next_id
        self.next_id += 1
        self.sock.sendall(_frame(get_request_body(rid, key)))
        body = _recv_frame(self.sock)
        tag = struct.unpack("<I", body[:4])[0]
        assert tag == TAG_GET_OK, "unexpected frame tag %d" % tag
        off = 4
        _, off = _parse_u64(body, off)
        present = body[off]
        off += 1
        if present == 0:
            return None
        n, off = _parse_u64(body, off)
        return body[off : off + n]

    def compact(self):
        self.sock.sendall(_frame(_u32(14)))
        body = _recv_frame(self.sock)
        tag = struct.unpack("<I", body[:4])[0]
        assert tag == TAG_COMPACT_DONE, "unexpected frame tag %d" % tag
        return struct.unpack("<Q", body[4:12])[0]

    def probe_fsyncs(self):
        self.sock.sendall(_frame(probe_request_body()))
        body = _recv_frame(self.sock)
        tag = struct.unpack("<I", body[:4])[0]
        assert tag == TAG_PROBE_RESP, "unexpected frame tag %d" % tag
        return struct.unpack("<Q", body[4:12])[0]


# --- verify ---------------------------------------------------------------


def verify(datadir):
    """Runs `shardvault-node --verify --dir` and returns the JSON report."""
    proc = subprocess.run(
        [str(NODE_BIN), "--verify", "--dir", str(datadir)],
        capture_output=True,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "verify failed (%d): %s" % (proc.returncode, proc.stderr.decode(errors="replace"))
        )
    return json.loads(proc.stdout.decode().strip().splitlines()[-1])


# --- crash matrix ----------------------------------------------------------


def probe_total_fsyncs(seed, workload, workdir):
    """Runs the workload on a healthy node and returns the total fsync count."""
    datadir = Path(workdir) / "probe"
    datadir.mkdir(parents=True)
    proc, addr = start_node(datadir, None)
    try:
        client = NodeClient(addr)
        for i, (key, value) in enumerate(workload):
            client.put(key, value)
            if i % 8 == 7:
                client.compact()
        count = client.probe_fsyncs()
        assert count > 0, (
            "node reported 0 fsyncs; build it with --features fault-injection"
        )
        return count
    finally:
        stop_node(proc)


def run_boundary(seed, workload, boundary, workdir):
    """Runs the workload with SV_FAIL_AT_FSYNC=boundary.

    Returns (acked, report) where `acked` maps key -> value for every put the
    node acknowledged before dying, and `report` is the verify JSON.
    """
    datadir = Path(workdir) / ("boundary-%04d" % boundary)
    datadir.mkdir(parents=True)
    proc, addr = start_node(datadir, boundary)
    acked = {}
    try:
        client = NodeClient(addr)
        for i, (key, value) in enumerate(workload):
            try:
                if client.put(key, value):
                    acked[key] = value
                else:
                    break
            except RuntimeError:
                break
            if i % 8 == 7:
                client.compact()
    except (socket.timeout, ConnectionError, OSError):
        pass
    finally:
        stop_node(proc)
    report = verify(datadir)
    return acked, report


def value_history(workload):
    """key -> ordered list of every value written for that key."""
    history = {}
    for key, value in workload:
        history.setdefault(key, []).append(value)
    return history


def check_invariants(workload, acked, report):
    """Returns a list of violation strings (empty = all invariants hold)."""
    violations = []
    history = value_history(workload)
    dump = {entry["key"]: entry for entry in report["keys"]}

    # 1. Store opens without error: implied by verify() succeeding.

    # 2. Every ACKed value survives: the recovered value for each key must
    #    be the acked version or a strictly later version that committed
    #    before the crash (its put was never ACKed).
    last_acked = {}
    for key, value in acked.items():
        last_acked[key] = value
    for key, value in last_acked.items():
        entry = dump.get(key)
        if entry is None:
            violations.append("acked key %s missing after recovery" % key)
            continue
        seq = history[key]
        pos = len(seq) - 1 - seq[::-1].index(value)
        candidates = seq[pos:]
        match = any(
            entry["len"] == len(v) and entry["crc"] == "%08x" % crc32c(v)
            for v in candidates
        )
        if not match:
            violations.append("acked key %s lost after recovery" % key)

    # 3. No torn or partial values: every present value must be one of the
    #    full values ever written for that key.
    for key, entry in dump.items():
        if key not in history:
            violations.append("unexpected key %s present after recovery" % key)
            continue
        match = any(
            entry["len"] == len(v) and entry["crc"] == "%08x" % crc32c(v)
            for v in history[key]
        )
        if not match:
            violations.append("torn or partial value for key %s" % key)

    # 4. Aggregates equal a brute-force recount of surviving objects
    #    (distinct keys).
    count = len(dump)
    total_bytes = sum(entry["len"] for entry in dump.values())
    agg = report["aggregate"]
    if agg["object_count"] != count or agg["byte_count"] != total_bytes:
        violations.append(
            "aggregate mismatch: report=%s brute=%s" % (agg, (count, total_bytes))
        )

    return violations


def run_matrix(seed, n_puts, workdir):
    """Runs the full fsync-boundary matrix. Returns (seed, M, results)."""
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    workload = gen_workload(seed, n_puts)
    m = probe_total_fsyncs(seed, workload, workdir)
    results = []
    for boundary in range(1, m + 1):
        acked, report = run_boundary(seed, workload, boundary, workdir)
        violations = check_invariants(workload, acked, report)
        recovered = len(report["keys"])
        count = len(report["keys"])
        total_bytes = sum(entry["len"] for entry in report["keys"])
        agg = report["aggregate"]
        delta = abs(agg["object_count"] - count) + abs(agg["byte_count"] - total_bytes)
        results.append(
            {
                "boundary": boundary,
                "violations": violations,
                "recovered": recovered,
                "aggregate": agg,
                "delta": delta,
            }
        )
    return seed, m, results
