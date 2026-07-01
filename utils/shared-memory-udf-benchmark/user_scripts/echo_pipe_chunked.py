#!/usr/bin/python3

# Fair per-chunk pipe baseline, using `send_chunk_header`: ClickHouse prefixes each chunk with
# its row count as a text line, so the command can read a whole chunk, echo it back and flush
# exactly once per chunk (instead of once per row). This isolates the transport cost (kernel copies
# through the pipe) from the per-row flush cost, making it the fairest pipe comparison to the
# shared-memory transport, which also exchanges one chunk per round-trip.

import sys


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    while True:
        header = stdin.readline()
        if not header:
            break  # STDIN closed -> exit
        num_rows = int(header)
        rows = [stdin.readline() for _ in range(num_rows)]
        stdout.write(b"".join(rows))
        stdout.flush()


if __name__ == "__main__":
    main()
