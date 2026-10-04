#!/usr/bin/python3
# A pooled command that answers correctly, waits until the query it answered is over (`--go`, see
# `go_signal.py`), and then exits non-zero while it sits in the pool.
#
# Nothing is waiting for a pooled process between borrows, so its exit is seen by nobody until the
# next query borrows it. What that query must not get is a failure of its own on the first write to
# a closed stdin, for something that happened before it started: the dead process has to be
# replaced before the borrow is built on it. It answers with its own pid, so a replacement is
# visible as a different one.
import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from go_signal import wait_for_go  # noqa: E402

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()
        if wait_for_go():
            sys.exit(1)
