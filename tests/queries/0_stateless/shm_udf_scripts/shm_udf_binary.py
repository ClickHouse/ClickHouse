#!/usr/bin/python3

# A shared-memory UDF speaking `RowBinary` rather than `TabSeparated`.
#
# The transport is documented as working with any `format`, and every other script here uses a
# line-oriented text one - which is exactly the format that would hide a framing bug. `RowBinary`
# carries embedded NUL bytes, a `Nullable` column with its own null map, and more than one column,
# so a byte lost or gained anywhere in the exchange shows up as a parse failure or a wrong value
# rather than being absorbed by a newline.
#
# Input:  UInt64 id, Nullable(String) label
# Output: String  round-tripped description
#
# `RowBinary` has no row count of its own: rows run until the input ends, which is precisely what
# the region's `size` says.

import mmap
import os
import struct
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


class Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def exhausted(self):
        return self.pos >= len(self.data)

    def take(self, n):
        chunk = self.data[self.pos : self.pos + n]
        if len(chunk) != n:
            raise ValueError("truncated RowBinary input")
        self.pos += n
        return chunk

    def varint(self):
        result = 0
        shift = 0
        while True:
            byte = self.take(1)[0]
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                return result
            shift += 7


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        version = read_varint(stdin)
        if version is None:
            break  # stdin closed -> exit
        if version != PROTOCOL_VERSION:
            raise RuntimeError(f"unsupported protocol version {version}")
        request_id = read_varint(stdin)

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
            reader = Reader(region[input_offset : input_offset + input_size])

            output = bytearray()
            while not reader.exhausted():
                (identifier,) = struct.unpack("<Q", reader.take(8))
                is_null = reader.take(1)[0]
                if is_null:
                    label = None
                else:
                    label = reader.take(reader.varint())

                if label is None:
                    described = b"#" + str(identifier).encode("ascii") + b"=<null>"
                else:
                    described = b"#" + str(identifier).encode("ascii") + b"=" + label

                output += encode_varint(len(described)) + described

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
