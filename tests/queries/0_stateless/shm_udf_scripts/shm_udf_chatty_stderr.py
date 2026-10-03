#!/usr/bin/python3

# A pooled UDF that writes a line to its `stderr` with every answer: after it has computed the
# answer, and before it sends the response frame. The line is therefore on the pipe before the
# server has the whole frame, and the server, which polls stderr together with stdout while it
# waits for the response, takes it off the pipe within the same invocation - so it is attributed to
# the query that caused it, whatever the reaction is, and never to the next one.
#
# A line written after the frame is a different case, and not one a test can pin down: once the
# server has the response it no longer waits for the command, so whether the line is in time for
# the check before the worker goes back to the pool is up to the scheduler. A line that misses that
# check is found by the next borrow, which is tested with a command that waits for it
# (`shm_udf_stderr_flood_after_gap.py`).
#
# It answers with its own pid, so a test can tell a fresh worker from a reused one.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0


def read_varint(stream):
    result = 0
    shift = 0
    while True:
        chunk = stream.read(1)
        if not chunk:
            return None
        byte = chunk[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result
        shift += 7


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    stderr = sys.stderr.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        request_id = read_varint(stdin)
        if version != PROTOCOL_VERSION:
            raise RuntimeError(f"unsupported protocol version {version}")

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        fd = os.open(path, os.O_RDWR)
        try:
            region = mmap.mmap(fd, 0)
        finally:
            os.close(fd)

        try:
            output = bytearray()
            for line in region[input_offset : input_offset + input_size].split(b"\n"):
                if line != b"":
                    output += str(os.getpid()).encode("ascii") + b"\n"

            output_offset = input_size
            region[output_offset : output_offset + len(output)] = bytes(output)
            region.flush()
        finally:
            region.close()

        # Before the frame, so that the server is still waiting for this response when the line is
        # on the pipe.
        stderr.write(b"done\n")
        stderr.flush()

        stdout.write(
            encode_varint(request_id)
            + encode_varint(STATUS_OK)
            + encode_varint(output_offset)
            + encode_varint(len(output))
        )
        stdout.flush()


if __name__ == "__main__":
    main()
