#!/usr/bin/python3

# Executable UDF that exchanges data through a shared-memory file instead of the pipes (the protocol
# is in `shm_protocol.py`). It answers every row of its `TabSeparated` input - one `UInt64` per line
# - with `Key <row>`, written right after the input, and exits when stdin reaches EOF. The flags
# change what it answers; the commands that also do something else are `shm_udf_noisy.py` (writes
# besides the answer), `shm_udf_region.py` (changes to the region's file) and `shm_udf_broken.py`
# (never answers properly).

import argparse
import os
import sys
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import key_row, pid_row, place_answer, serve  # noqa: E402

parser = argparse.ArgumentParser()
# Each row is answered with itself, so the query decides whether the answer parses as the return type.
parser.add_argument("--echo", action="store_true")
parser.add_argument("--report-pid", action="store_true")
# A function without arguments: the request carries no input at all (offset 0, size 0) and the answer
# is one row that depends on none. The server still has to make that request - there is nothing else
# that would make this command produce a row.
parser.add_argument("--zero-argument", action="store_true")
# Answers the first row only: too few rows.
parser.add_argument("--first-row-only", action="store_true")
# Answers every row twice: more rows than requested. The server must detect the overproduction, fail
# the query with a "wrong result" error, and (for `executable_pool`) invalidate the worker instead of
# returning it to the pool as valid.
parser.add_argument("--duplicate-rows", action="store_true")
# Writes the answer at offset 0, over the already-consumed input, instead of right after it. With
# `--echo` the answer is exactly as large as the input, so it always fits into a region that the
# server has just grown to hold the input - which lets a test drive region growth without also having
# to reserve room for a larger output.
parser.add_argument("--answer-at-start", action="store_true")
# A line on stderr before the first request is ever read: what a command that logs its startup
# writes. It belongs to the query that started the process, and under `stderr_reaction` `throw` it
# fails that query - the process is new, so there is no earlier invocation for the server to pin it on.
parser.add_argument("--stderr-at-startup", action="store_true")
# Takes its time over cleanup: on stdin EOF - the server telling it to exit - it stays alive for the
# given number of seconds, past `command_termination_timeout`, and only then exits successfully. A
# non-pooled command is waited for until it exits when its exit code is checked, whatever transport it
# uses, so such a command passes.
parser.add_argument("--linger", type=float, metavar="SECONDS")
args = parser.parse_args()


def answer_row(row):
    if args.echo:
        output = row + b"\n"
    elif args.report_pid:
        output = pid_row(row)
    else:
        output = key_row(row)
    return output * 2 if args.duplicate_rows else output


def answer(input_data, region):
    if args.zero_argument:
        return place_answer(region, len(input_data), b"42\n")

    rows = [row for row in input_data.split(b"\n") if row != b""]
    if args.first_row_only:
        rows = rows[:1]
    output = b"".join(answer_row(row) for row in rows)
    return place_answer(region, 0 if args.answer_at_start else len(input_data), output)


def main():
    if args.stderr_at_startup:
        sys.stderr.write("starting up\n")
        sys.stderr.flush()

    serve(answer)

    if args.linger is not None:
        time.sleep(args.linger)


if __name__ == "__main__":
    main()
