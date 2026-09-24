#!/usr/bin/env python3
"""Tiny ShardVault client CLI (see README quickstart).

Usage:
  python3 harness/cli.py --addr HOST:PORT put KEY VALUE
  python3 harness/cli.py --addr HOST:PORT get KEY
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import crash  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="ShardVault client")
    parser.add_argument("--addr", required=True)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("put")
    p.add_argument("key")
    p.add_argument("value")

    g = sub.add_parser("get")
    g.add_argument("key")

    args = parser.parse_args()
    client = crash.NodeClient(args.addr)
    if args.cmd == "put":
        ok = client.put(args.key, args.value.encode())
        print("ack" if ok else "no ack (node crashed?)")
        return 0 if ok else 1
    value = client.get(args.key)
    if value is None:
        print("(none)")
    else:
        print(value.decode(errors="replace"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
