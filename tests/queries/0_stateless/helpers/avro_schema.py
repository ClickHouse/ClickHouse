#!/usr/bin/env python3
"""Prints the writer schema JSON of an Avro object container file: avro_schema.py FILE."""

import sys


def read_long(f):
    shift = result = 0
    while True:
        byte = f.read(1)[0]
        result |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return (result >> 1) ^ -(result & 1)


# The header is the magic, then a map of metadata entries in blocks; a negative block count is followed by its size.
def read_schema(path):
    with open(path, "rb") as f:
        if f.read(4) != b"Obj\x01":
            raise ValueError(f"{path} is not an Avro object container file")
        while (count := read_long(f)) != 0:
            if count < 0:
                count = -count
                read_long(f)
            for _ in range(count):
                key = f.read(read_long(f)).decode()
                value = f.read(read_long(f))
                if key == "avro.schema":
                    return value.decode()
    raise ValueError(f"{path} has no avro.schema")


if __name__ == "__main__":
    print(read_schema(sys.argv[1]))
