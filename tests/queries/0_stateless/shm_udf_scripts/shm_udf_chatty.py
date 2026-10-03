#!/usr/bin/python3

# Misbehaving pooled UDF: it answers every request correctly and then writes one more byte to its
# `stdout` - the accident a stray `print` or a forgotten newline is. Nothing in the exchange notices
# it, because the answer itself was read in full and the result lives in the shared-memory region;
# the byte is only ever read by the *next* request on the same process, as the status varint of a
# response that has not been sent yet. The server must therefore refuse to return this worker to the
# pool, so the damage stays inside the invocation that caused it.

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
                    output += b"Key " + line + b"\n"

            output_offset = input_size
            region[output_offset : output_offset + len(output)] = bytes(output)
            region.flush()
        finally:
            region.close()

        # The response frame, and after it a byte that is not part of it and not part of anything
        # else - in one write, so that the byte is in the pipe by the time the server has read the
        # frame out of it. Written field by field, it would go out in separate writes wherever
        # `stdout` is unbuffered (`PYTHONUNBUFFERED`, set in CI), and the server could finish the
        # invocation and pronounce the worker clean before this process wrote the byte. A worker
        # discarded for that only at its next borrow is not counted by a query that never comes.
        stdout.write(
            encode_varint(request_id)
            + encode_varint(STATUS_OK)
            + encode_varint(output_offset)
            + encode_varint(len(output))
            + b"\n"
        )
        stdout.flush()


if __name__ == "__main__":
    main()
