#!/usr/bin/python3

# A pooled command with two inputs that answers one request, closes its stdout and only exits once
# both of its inputs have reached EOF - which is how a command written to drain its input behaves.
#
# The server discards a worker that has hung up its stdout and, under `check_exit_code`, waits for
# its exit status. For that wait to end, every descriptor the command reads from has to be closed,
# not only its stdin: a teardown that closed stdin alone would leave this process waiting on its
# second input, sit out the whole `command_termination_timeout`, and fail the query for an exit
# code that was a `close` away.

import os
import sys


def read_chunk(stream):
    header = stream.readline()
    if not header:
        return
    for _ in range(int(header)):
        stream.readline()


if __name__ == "__main__":
    second_input = os.fdopen(3)

    read_chunk(sys.stdin)
    read_chunk(second_input)

    print("1")
    print("answered")
    sys.stdout.flush()
    os.close(sys.stdout.fileno())

    # Not finished while any input is still open.
    sys.stdin.read()
    second_input.read()
