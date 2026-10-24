#!/usr/bin/python3

# The source of an executable dictionary: produces three rows, and then exits successfully - unless the
# flags say otherwise.

import argparse
import os
import sys
import time

parser = argparse.ArgumentParser()
# Writes a diagnostic to stderr before the rows - not merely before they are flushed, since CI runs
# Python with `PYTHONUNBUFFERED`, where every write goes straight to the pipe - so it is already waiting
# whenever the server has the rows, and what the server does about it does not depend on scheduling.
parser.add_argument("--stderr", action="store_true")
# Exits with this code after the rows.
parser.add_argument("--exit-code", type=int, default=0)
# Closes its stdout after the rows and stays alive this long - longer than the
# `command_termination_timeout` it is configured with - before it exits. `sys.stdout.close` would
# leave the descriptor open (the standard streams are opened with `closefd=False`); the server only
# sees EOF once the descriptor itself is closed.
parser.add_argument("--linger", type=float, metavar="SECONDS")
args = parser.parse_args()

if __name__ == "__main__":
    if args.stderr:
        sys.stderr.write("the source complains\n")
        sys.stderr.flush()

    for key in range(1, 4):
        print(f"{key}\tValue {key}")
    sys.stdout.flush()

    if args.linger is not None:
        os.close(sys.stdout.fileno())
        time.sleep(args.linger)

    sys.exit(args.exit_code)
