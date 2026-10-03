#!/usr/bin/python3

# A well-behaved pooled UDF that closes its own `stderr` once it is up. Commands do this - a script
# that daemonizes its logging, or simply does not want the descriptor - and it is entirely legal:
# the protocol lives on `stdin`/`stdout` and the shared-memory region.
#
# It exists to pin down the difference between the two things a pipe can report. Once the only
# writer closes it, `stderr` polls as `POLLHUP` forever, with no `POLLIN` and nothing to read. A
# reuse check that asked only "is anything pending?" would read that hangup as leftover output and
# throw this worker away on every single borrow, turning `executable_pool` into a process per call -
# quietly, because the queries themselves would all still succeed.
#
# It answers with its own pid, so a test can tell a reused worker from a fresh one.

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


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    # Before answering anything, so every borrow sees a stderr that is closed and will stay closed.
    os.close(2)

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

        write_varint(stdout, request_id)
        write_varint(stdout, STATUS_OK)
        write_varint(stdout, output_offset)
        write_varint(stdout, len(output))
        stdout.flush()


if __name__ == "__main__":
    main()
