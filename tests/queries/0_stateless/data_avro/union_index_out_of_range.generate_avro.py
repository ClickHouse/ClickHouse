#!/usr/bin/env python3
"""Generates union_index_out_of_range_*.avro: valid Avro container files except that one union index
is past the last branch of its union. Uses only the standard library: Avro encoders cannot write an invalid index."""
import json

SYNC = bytes(range(16))


def long_(n):
    n = ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def string(s):
    b = s.encode()
    return long_(len(b)) + b


def write(name, fields, rows):
    schema = json.dumps({"type": "record", "name": "row", "fields": fields}).encode()
    meta = long_(2) + string("avro.schema") + long_(len(schema)) + schema + string("avro.codec") + string("null") + long_(0)
    data = b"".join(rows)
    with open(name, "wb") as f:
        f.write(b"Obj\x01" + meta + SYNC + long_(len(rows)) + long_(len(data)) + data + SYNC)


# The row ("x") has union index 10; the union has 3 branches.
write("union_index_out_of_range_variant.avro", [{"name": "u", "type": ["null", "int", "string"]}], [long_(10) + string("x")])
# Rows ("x", 7), ("yz", 8); the first union index is 10, the union has 2 branches.
write("union_index_out_of_range_nullable.avro", [{"name": "u", "type": ["null", "string"]}, {"name": "n", "type": "int"}],
      [long_(10) + string("x") + long_(7), long_(1) + string("yz") + long_(8)])
# A one-branch union with index 1.
write("union_index_out_of_range_single.avro", [{"name": "u", "type": ["string"]}], [long_(1) + string("x")])
