#!/usr/bin/python3

# A pooled command over the pipes that answers every row of its `TabSeparated` input with its own pid,
# so that a test can tell a reused worker from a fresh one - a replacement is visible as a different
# one - and writes something besides: with the row, right after it, or once the query it answered is
# over (`--go`, see `go_signal.py`). What, and when, is what the flags say. `shm_udf_noisy.py` is the
# shared-memory counterpart.

import argparse
import fcntl
import os
import sys
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from go_signal import wait_for_go  # noqa: E402


def write(stream, text):
    stream.write(text)
    stream.flush()


def touch_marker():
    if args.marker:
        with open(args.marker, "w"):
            pass


def late_exit():
    # Exits non-zero while it sits in the pool. Nothing is waiting for a pooled process between
    # borrows, so its exit is seen by nobody until the next query borrows it. What that query must not
    # get is a failure of its own on the first write to a closed stdin, for something that happened
    # before it started: the dead process has to be replaced before the borrow is built on it.
    sys.exit(1)


def last_words():
    # Writes a diagnostic to stderr and exits. Nobody is reading its pipes by then: the query it served
    # is over and the process sits idle. The next borrow finds it dead and replaces it - and must report
    # what it wrote on the way out rather than drop it with the process.
    write(sys.stderr, "last words of the worker\n")
    sys.exit(0)


def late_row():
    # Writes an extra row to its stdout, and then creates the file named by `--marker`, so that the test
    # borrows the worker again only once the row is on the pipe. With `--linger` it then stays until it
    # is killed, rather than exit on stdin EOF.
    #
    # The probe when the worker is handed back finds an empty pipe and cannot say anything about what
    # comes next. That leaves the row waiting for whoever borrows this process next - and the pipe
    # transport has no framing that would let that query tell a stale row from its own. Parsed as its
    # first row, it is a silently wrong answer, which is exactly what a borrow must refuse to start on.
    write(sys.stdout, "999999\n")
    touch_marker()
    if args.linger:
        while True:
            time.sleep(1)


def stderr_flood():
    # Writes to stderr: by default more than a pipe can hold, for `stderr_reaction` `none`, which
    # promises that a chatty command never blocks on a full stderr pipe. The gap defeats any check that
    # only looks at the pipe at the moment the worker is handed back; what keeps the promise is that the
    # read loop of the next borrow polls stderr alongside stdout, so the query waiting for a response is
    # the one that unblocks the command writing it.
    #
    # `--bytes N --marker PATH` writes at most `N` bytes instead, and no more than the pipe holds, and
    # then creates the file: the whole burst is on the pipe, and the command back at its next request,
    # by the time the file exists - a state the test can wait for, which a burst still being written is
    # not. What the pipe holds is read rather than assumed: once a user holds more pipe pages than
    # `pipe-user-pages-soft` allows - a machine running many tests at once - the kernel gives new pipes a
    # single page and refuses to enlarge them, and a burst sized for the default would block halfway.
    size = args.bytes
    if args.marker:
        size = min(size, fcntl.fcntl(sys.stderr.fileno(), fcntl.F_GETPIPE_SZ))
    write(sys.stderr, "e" * size)
    touch_marker()


AFTER_GO = {
    "exit": late_exit,
    "last-words": last_words,
    "stdout-row": late_row,
    "stderr-flood": stderr_flood,
}

parser = argparse.ArgumentParser()
# One byte too many right after the answer, in the same write. The byte arrives with the answer, so the
# server reads it into its own buffer along with the rows, and it dies with that buffer: it never
# reaches the pipe the next borrower reads, and the worker is still at a usable boundary. A newline, so
# that a next borrow reading it would see a plausible - and empty - first row rather than a parse error.
# Flushed on its own, it could reach the pipe only after the server finished reading, and whether the
# worker is kept would be decided by the scheduler. A byte written later, once the server has stopped
# reading, is the other case (`--after-go stdout-row`).
parser.add_argument("--extra-newline", action="store_true")
# A line on stderr after every row, so that it lands on the pipe after the server has read its answer.
# Under a `log*` reaction that line is a log line, not a verdict, and must not cost the command its
# process.
parser.add_argument("--stderr-after-rows", action="store_true")
parser.add_argument("--go", metavar="PATH")
# What the command does once the query it answered is over (`--go`), skipped if the next request (or
# the end of stdin) comes first.
parser.add_argument("--after-go", choices=AFTER_GO)
parser.add_argument("--marker", metavar="PATH")
parser.add_argument("--linger", action="store_true")
parser.add_argument("--bytes", type=int, default=128 * 1024)
# Shrinks the stderr pipe to this many pages at startup.
parser.add_argument("--pipe-pages", type=int)
args = parser.parse_args()


def main():
    if args.pipe_pages:
        fcntl.fcntl(sys.stderr.fileno(), fcntl.F_SETPIPE_SZ, args.pipe_pages * os.sysconf("SC_PAGE_SIZE"))

    for line in sys.stdin:
        if not line.strip():
            continue
        write(sys.stdout, f"{os.getpid()}\n" + ("\n" if args.extra_newline else ""))
        if args.stderr_after_rows:
            write(sys.stderr, "logging right after the rows\n")
        if args.after_go and wait_for_go():
            AFTER_GO[args.after_go]()


if __name__ == "__main__":
    main()
