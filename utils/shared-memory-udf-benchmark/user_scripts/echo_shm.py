#!/usr/bin/python3

# Shared-memory transport, echo: the bulk data lives in a shared-memory file that both ClickHouse
# and this process mmap; the pipes carry only small control commands. For each chunk the server
# writes the serialized input into the region and sends {version, path, offset, size}; this process
# maps the file, echoes the input back at offset 0 (over the already-consumed input) and replies
# with {status, offset, size}. The echo output is exactly as large as the input, so it always fits.
# See docs/reference/functions/regular-functions/udf.mdx ("Shared memory mode") for the protocol.

import mmap
import os
import sys

PROTOCOL_VERSION = 1
STATUS_OK = 0
STATUS_ERROR = 1


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


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit

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
