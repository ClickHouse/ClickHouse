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

import os
import struct
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import encode_varint, place_answer, serve  # noqa: E402


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


def answer(input_data, region):
    reader = Reader(input_data)
    output = bytearray()
    while not reader.exhausted():
        (identifier,) = struct.unpack("<Q", reader.take(8))
        is_null = reader.take(1)[0]
        label = None if is_null else reader.take(reader.varint())

        described = b"#" + str(identifier).encode("ascii") + b"=" + (b"<null>" if label is None else label)
        output += encode_varint(len(described)) + described

    return place_answer(region, len(input_data), output)


if __name__ == "__main__":
    serve(answer)
