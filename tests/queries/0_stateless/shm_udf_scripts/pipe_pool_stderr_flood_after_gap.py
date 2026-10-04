#!/usr/bin/python3
# The pipe-mode twin of `shm_udf_stderr_flood_after_gap.py`: a pooled command that answers, waits
# until it has been handed back to the pool - hand-back drain and all (`--go`, see `go_signal.py`) -
# and only then writes to `stderr`.
#
# By default it writes more than a pipe can hold, for `stderr_reaction` `none`, which promises that
# a chatty command never blocks on a full `stderr` pipe. The gap defeats any check that only looks
# at the pipe at the moment the worker is handed back; what keeps the promise is that the read loop
# of the next borrow polls `stderr` alongside `stdout`, so the query waiting for a response is the
# one that unblocks the command writing it.
#
# `--bytes N --marker PATH` writes at most `N` bytes instead, and no more than the pipe holds, and
# then creates the file: the whole burst is on the pipe, and the command back at its next request, by
# the time the file exists - a state the test can wait for, which a burst still being written is not.
# What the pipe holds is read rather than assumed: once a user holds more pipe pages than
# `pipe-user-pages-soft` allows - a machine running many tests at once - the kernel gives new pipes a
# single page and refuses to enlarge them, and a burst sized for the default would block halfway.
#
# Answers with its own pid, so a reused worker is visible as a repeated one.
import fcntl
import os
import sys

from go_signal import wait_for_go

if __name__ == "__main__":
    size = int(sys.argv[sys.argv.index("--bytes") + 1]) if "--bytes" in sys.argv else 128 * 1024
    marker = sys.argv[sys.argv.index("--marker") + 1] if "--marker" in sys.argv else None
    if marker:
        size = min(size, fcntl.fcntl(sys.stderr.fileno(), fcntl.F_GETPIPE_SZ))
    for line in sys.stdin:
        if not line.strip():
            continue
        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()
        if wait_for_go():
            sys.stderr.write("e" * size)
            sys.stderr.flush()
            if marker:
                with open(marker, "w"):
                    pass
