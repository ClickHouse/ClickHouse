#!/usr/bin/python3

# The pipe counterpart of echo_shm_busy.py: the per-chunk pipe echo of echo_pipe_chunked.py with the
# same artificial CPU work added before the reply, so that a "busy" command can be compared across
# the two transports doing the same work. See echo_shm_busy.py for the knob (SHM_UDF_BUSY_ITERS).
#
# Fair per-chunk pipe baseline, using `send_chunk_header`: ClickHouse prefixes each chunk with its
# row count as a text line, so the command can read a whole chunk, echo it back and flush exactly
# once per chunk (instead of once per row). This isolates the transport cost (kernel copies through
# the pipe) from the per-row flush cost, making it the fairest pipe comparison to the shared-memory
# transport, which also exchanges one chunk per round-trip.
#
# The rows are moved in bulk, never one Python object per row: the shared-memory client copies the
# whole serialized block with a single slice, so a `readline`-per-row loop here would charge the
# pipe transport for Python parsing and allocation that the other side never does (measurably so:
# for a 65k-row block that loop costs milliseconds, while the payload copy costs microseconds).
# Both clients therefore do one bulk read and one bulk write per chunk, and the measured difference
# stays attributable to the transport.

import os
import sys

READ_SIZE = 1 << 20
BUSY_ITERS = int(os.environ.get("SHM_UDF_BUSY_ITERS", "400000"))


def busy(seed):
    # Identical to echo_shm_busy.py: a fixed-size integer mixing loop, run once per chunk.
    acc = seed & 0xFFFFFFFF
    for i in range(BUSY_ITERS):
        acc = (acc * 1315423911 + i) & 0xFFFFFFFF
    return acc


def end_of_chunk(buffer, num_rows, newlines):
    """Offset just past the `num_rows`-th newline of `buffer`, which holds `newlines` of them.

    Walks back over the newlines that already belong to the next chunk, so the split costs one
    backwards scan of the read-ahead tail rather than a search per row.
    """
    end = len(buffer)
    for _ in range(newlines - num_rows + 1):
        end = buffer.rfind(b"\n", 0, end)
    return end + 1


def main():
    # `os.read` hands over whatever has already arrived, while a buffered `read(n)` waits for the
    # full n bytes - and the server is waiting for this chunk's answer, so that wait never ends.
    stdin = sys.stdin.fileno()
    stdout = sys.stdout.buffer

    buffer = b""  # bytes already read past the current position
    while True:
        while b"\n" not in buffer:
            block = os.read(stdin, READ_SIZE)
            if not block:
                return  # STDIN closed -> exit
            buffer += block

        header, _, buffer = buffer.partition(b"\n")
        num_rows = int(header)

        parts = [buffer]
        newlines = buffer.count(b"\n")
        while newlines < num_rows:
            block = os.read(stdin, READ_SIZE)
            if not block:
                raise RuntimeError(f"STDIN ended in the middle of a chunk of {num_rows} rows")
            parts.append(block)
            newlines += block.count(b"\n")

        if len(parts) > 1:
            buffer = b"".join(parts)

        end = end_of_chunk(buffer, num_rows, newlines)
        busy(end)  # simulate heavy per-chunk compute
        stdout.write(buffer[:end])
        stdout.flush()
        buffer = buffer[end:]


if __name__ == "__main__":
    main()
