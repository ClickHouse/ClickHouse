#!/usr/bin/python3
# A pooled worker that answers, waits until it has been handed back to the pool (`--go`, see
# `go_signal.py`), then writes a diagnostic to stderr and exits. Nobody is reading its pipes by
# then: the query it served is over and the process sits idle. The next borrow finds it dead and
# replaces it - and must report what it wrote on the way out rather than drop it with the process.
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
            sys.stderr.write("last words of the worker\n")
            sys.stderr.flush()
            sys.exit(0)
