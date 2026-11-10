#!/usr/bin/python3

# A shared-memory UDF (see `shm_protocol.py`) that reads its requests and never answers one properly.
# How it fails is what the one flag given says; the server must surface an error rather than hang or
# return wrong results.

import argparse
import os
import sys
import time

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import error_frame, ok_frame, read_request  # noqa: E402

parser = argparse.ArgumentParser()
mode = parser.add_mutually_exclusive_group(required=True)
# Reads a request and then exits without answering, simulating a crashed command.
mode.add_argument("--die", action="store_true")
# Reports success with an output region past the end of the shared-memory region. The server must
# reject the response instead of reading past the region.
mode.add_argument("--bad-offset", action="store_true")
# Answers through the protocol's error channel (a non-zero status followed by a length-prefixed
# message). The server must fail the query with that message.
mode.add_argument("--error", action="store_true")
# Answers with an error frame of a message of the given length, written one byte at a time with the
# given pause before each: every pause is shorter than `command_read_timeout`, the whole frame takes
# longer.
mode.add_argument("--drip", nargs=2, type=float, metavar=("PAUSE", "LENGTH"))
# Reads the request and never writes a byte to stdout, without exiting either; the server has to time
# it out with `command_read_timeout`.
#
# `quiet` closes its stderr first. Once the only writer is gone, `poll` reports `POLLHUP` on that pipe
# immediately and forever, and a read of it returns zero bytes. A server that keeps that descriptor in
# the set it waits on therefore never waits at all: it spins, burning a core, and the timeout - which
# is measured by the poll it is no longer doing - never fires. So the descriptor has to be dropped
# from the poll set at EOF.
#
# `chatty` keeps talking on its stderr instead, far more often than the timeout. Both descriptors are
# polled - `stderr_reaction` `none` still has to take those bytes off the pipe, or the command blocks
# in `write` - so every one of these lines wakes that wait up. A wait that restarts its budget at each
# wake-up is no longer bounded by anything the command does not control, so the timeout has to be one
# budget for the whole read. The pause between lines is what makes the wake-ups a stream of separate
# events rather than one long readable stretch, which is the shape that resets a per-wake-up budget.
mode.add_argument("--stall", choices=["quiet", "chatty"])
args = parser.parse_args()


def stall():
    while True:
        if args.stall == "chatty":
            sys.stderr.buffer.write(b"still here\n")
            sys.stderr.buffer.flush()
        time.sleep(0.05)


def write_answer(stdout, request):
    if args.bad_offset:
        # Success status, but the output claims to live past the end of the region.
        stdout.write(ok_frame(request.request_id, os.path.getsize(request.path) + 1024, 16))
    elif args.error:
        stdout.write(error_frame(request.request_id, "the command cannot process this request"))
    else:
        pause, length = args.drip
        frame = error_frame(request.request_id, "x" * int(length))
        for i in range(len(frame)):
            time.sleep(pause)
            stdout.write(frame[i : i + 1])
            stdout.flush()
    stdout.flush()


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    if args.stall == "quiet":
        # Before anything else, so the pipe is already hung up when the server starts waiting.
        os.close(2)

    request = read_request(stdin)
    if args.die:
        sys.exit(0 if request is None else 1)
    if args.stall:
        stall()

    while request is not None:
        write_answer(stdout, request)
        request = read_request(stdin)


if __name__ == "__main__":
    main()
