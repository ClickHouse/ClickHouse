#!/usr/bin/python3
# A pooled pipe-mode command that answers correctly, waits until it has been handed back to the
# pool (`--go`, see `go_signal.py`), and only then writes an extra row to its `stdout`.
#
# The gap is what makes it different from `pipe_pool_chatty.py`: the probe when the worker is
# handed back finds an empty pipe and cannot say anything about what comes next. That leaves the
# row waiting for whoever borrows this process next - and the pipe transport has no framing that
# would let that query tell a stale row from its own. Parsed as its first row, it is a silently
# wrong answer, which is exactly what a borrow must refuse to start on.
#
# The file named by `--marker` is created once the row is on the pipe, so that the test borrows the
# worker again only then.
import os
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from go_signal import wait_for_go  # noqa: E402

if __name__ == "__main__":
    marker = sys.argv[sys.argv.index("--marker") + 1]
    for line in sys.stdin:
        if not line.strip():
            continue
        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()
        if wait_for_go():
            sys.stdout.write("999999\n")
            sys.stdout.flush()
            with open(marker, "w"):
                pass
