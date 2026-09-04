#!/usr/bin/python3

# Same shared-memory echo transport as echo_shm.py, but with artificial per-chunk CPU work added
# before replying. This models a UDF whose per-chunk compute time dominates the server-side
# serialization time, so that the benchmark measures a command doing real work rather than the
# transport alone.
#
# The amount of work is controlled by the SHM_UDF_BUSY_ITERS environment variable (default 400000):
# a fixed integer mixing loop run once per chunk, so per-chunk compute is substantial and tunable.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0
STATUS_ERROR = 1

BUSY_ITERS = int(os.environ.get("SHM_UDF_BUSY_ITERS", "400000"))


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


def write_varint(stream, value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    stream.write(bytes(out))


def write_string_binary(stream, text):
    encoded = text.encode("utf-8")
    write_varint(stream, len(encoded))
    stream.write(encoded)


def busy(seed):
    # A fixed-size integer mixing loop, run once per chunk. Kept independent of the chunk size so
    # the per-chunk compute time is stable and controllable via SHM_UDF_BUSY_ITERS.
    acc = seed & 0xFFFFFFFF
    for i in range(BUSY_ITERS):
        acc = (acc * 1315423911 + i) & 0xFFFFFFFF
    return acc


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break

        path_length = read_varint(stdin)
        path = stdin.read(path_length).decode("utf-8")
        input_offset = read_varint(stdin)
        input_size = read_varint(stdin)

        try:
            if version != PROTOCOL_VERSION:
                raise ValueError(f"unsupported protocol version {version}")

            fd = os.open(path, os.O_RDWR)
            try:
                region = mmap.mmap(fd, 0)
            finally:
                os.close(fd)

            try:
                data = bytes(region[input_offset : input_offset + input_size])
                busy(input_size)  # simulate heavy per-chunk compute
                region[0:input_size] = data
                output_offset, output_size = 0, input_size
            finally:
                region.close()

            write_varint(stdout, STATUS_OK)
            write_varint(stdout, output_offset)
            write_varint(stdout, output_size)
        except Exception as exception:  # noqa: BLE001
            write_varint(stdout, STATUS_ERROR)
            write_string_binary(stdout, str(exception))

        stdout.flush()


if __name__ == "__main__":
    main()
