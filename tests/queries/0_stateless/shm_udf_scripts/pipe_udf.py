#!/usr/bin/python3

# A command over the pipes that answers every row of its `TabSeparated` input with `Key <row>`, and
# exits successfully once its stdin reaches EOF. The flags say how it answers, and what it does on its
# way out, in the order they are listed here. Every write goes straight to the pipe (`os.write`), as
# it does in CI, where Python runs with `PYTHONUNBUFFERED`.

import argparse
import os
import sys
import time

parser = argparse.ArgumentParser()
# Answers with `<row>\tKey <row>`: the row of an executable dictionary that looks keys up.
parser.add_argument("--with-key", action="store_true")
# Answers the first row only and reads no further.
parser.add_argument("--first-row-only", action="store_true")
# The answer without its trailing newline: its last field then ends at EOF, so the server cannot
# finish reading it before stdout is closed (`--close-stdout`), and by the time it decides whether to
# keep the worker the hangup is already there to be seen. With the newline, the answer would be
# complete before the close, and a server that checked in between would put the worker back in the
# pool as healthy - and the request would not wait for it at all.
parser.add_argument("--no-newline", action="store_true")
# A diagnostic on stderr with every row, written before the row - before it is written at all, not
# merely before it is flushed - so it is already waiting whenever the server has the row, and what the
# server can act on is deterministic. The other order - the row, then the diagnostic - would leave a
# window in which the server has its rows, looks at stderr once, and the diagnostic is still on the
# way; a command that lost the CPU in that window would pass, and the test would flake.
#
# For a pooled command under `stderr_reaction` `throw`, which promises that anything the command writes
# to stderr fails the query, that is the one path where nothing looks at its stderr again: a worker
# that satisfied the row count is handed straight back to the pool without being waited for. The
# setting would then cost the command a worker while telling the user nothing.
parser.add_argument("--stderr-with-rows", action="store_true")
# On its way out: ends its output, so that the server has the whole answer and is only waiting for this
# process to go. `sys.stdout.close` would leave the descriptor open (the standard streams are opened
# with `closefd=False`); the server only sees EOF once the descriptor itself is closed.
parser.add_argument("--close-stdout", action="store_true")
# Writes its pid to the file before anything else, for the test to check that the server did not
# leave it behind as a zombie - before the answer, so that it is there however soon the server ends
# the process after it.
parser.add_argument("--pid-file", metavar="PATH")
# Then reads its stdin to the end - the way a pooled command is written to exit. A pooled command that
# closed its stdout cannot go back to the pool, so its exit code is read right after its answer;
# whether the server lets it see the end of its stdin first is the difference between an exit within a
# moment and a `command_termination_timeout` sat out.
parser.add_argument("--read-to-eof", action="store_true")
# Or refuses to leave instead: it sleeps far past any `command_termination_timeout` here, and only then
# exits non-zero - which nothing is to see, because the server is to end it before.
parser.add_argument("--linger", action="store_true")
# Then sleeps this long: past the drain's idle window (100 ms) or `command_termination_timeout`.
parser.add_argument("--pause", type=float, metavar="SECONDS")
# Then writes one stray line to stdout. Without `--close-stdout`, the stray write must not be what
# kills the process: if the server closed its stdout as soon as it had the rows, that write would die
# on `SIGPIPE`, with the diagnostic of `--stderr-on-the-way-out` still unwritten.
parser.add_argument("--stray-stdout", action="store_true")
# Then writes a diagnostic to stderr. With `--close-stdout --pause`, the line is written well after
# the drain that runs when stdout ends has stopped, so it is found only by the bounded wait that reaps
# the command - the last stretch in which a command can write anything at all. Those bytes are still
# output the command produced, and `stderr_reaction` `throw` promises that output fails the query -
# with the exit-status check switched off too.
parser.add_argument("--stderr-on-the-way-out", action="store_true")
# The code it finally exits with: an exit status that `check_exit_code` fails the query for, which must
# not hide what it wrote on its way out.
parser.add_argument("--exit-code", type=int, default=0)
args = parser.parse_args()


def main():
    if args.pid_file:
        with open(args.pid_file, "w") as f:
            f.write(f"{os.getpid()}\n")

    rows = [sys.stdin.readline()] if args.first_row_only else sys.stdin
    for row in rows:
        row = row.strip()
        if not row:
            continue
        if args.stderr_with_rows:
            os.write(2, b"the command complains\n")
        answer = f"{row}\tKey {row}" if args.with_key else f"Key {row}"
        os.write(1, (answer if args.no_newline else answer + "\n").encode())

    if args.close_stdout:
        os.close(1)
    if args.read_to_eof:
        for _ in sys.stdin:
            pass
    if args.linger:
        time.sleep(600)
        sys.exit(1)
    if args.pause:
        time.sleep(args.pause)
    if args.stray_stdout:
        os.write(1, b"stray line\n")
    if args.stderr_on_the_way_out:
        os.write(2, b"complaining on the way out\n")
    sys.exit(args.exit_code)


if __name__ == "__main__":
    main()
