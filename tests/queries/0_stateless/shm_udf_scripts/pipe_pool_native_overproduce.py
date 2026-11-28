#!/usr/bin/python3
# A pooled worker over the pipes speaking the `Native` format that answers every block with one
# row more than it was asked for. A block format hands the server the command's block whole, so
# the extra row arrives inside the same chunk as the requested ones - the one shape of
# overproduction that a row limit on the format cannot catch, and that the server has to catch on
# the row count itself, before the chunk leaves the source and before the worker goes back to the
# pool as if it had answered correctly.
import os
import struct
import sys

# CI runs Python with `PYTHONSAFEPATH`, which keeps the script's own directory out of `sys.path`.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from shm_protocol import read_varint  # noqa: E402


# Unlike the shared one, appends to a `bytearray`: the answer block is built whole, then written.
def write_varint(out, value):
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return


def read_string(stream):
    length = read_varint(stream)
    return stream.read(length)


def main():
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    while True:
        # One `Native` block: columns, rows, then per column its name, type and data.
        num_columns = read_varint(stdin)
        if num_columns is None:
            return
        num_rows = read_varint(stdin)
        values = []
        for _ in range(num_columns):
            read_string(stdin)  # name
            column_type = read_string(stdin)
            assert column_type == b"UInt64", column_type
            values = [struct.unpack("<Q", stdin.read(8))[0] for _ in range(num_rows)]

        # The answer, with one row too many.
        rows = [f"Key {value}".encode() for value in values] + [b"one row too many"]
        out = bytearray()
        write_varint(out, 1)
        write_varint(out, len(rows))
        write_varint(out, len(b"result"))
        out += b"result"
        write_varint(out, len(b"String"))
        out += b"String"
        for row in rows:
            write_varint(out, len(row))
            out += row
        stdout.write(bytes(out))
        stdout.flush()


if __name__ == "__main__":
    main()
