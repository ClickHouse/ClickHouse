#!/usr/bin/python3

# Baseline pipe transport, streaming: read one line from STDIN, echo it to STDOUT, flush.
# This is the pattern from the executable-UDF documentation. It flushes per row so that
# ClickHouse can read results back incrementally, which is the realistic behaviour of a
# streaming pipe UDF that does not know chunk boundaries.

import sys


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    for line in stdin:
        stdout.write(line)
        stdout.flush()


if __name__ == "__main__":
    main()
