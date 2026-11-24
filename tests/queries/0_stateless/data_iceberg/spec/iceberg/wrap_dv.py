#!/usr/bin/env python3
"""Wraps a deletion vector into a Puffin file with a single `deletion-vector-v1` blob.

Usage: wrap_dv.py <payload> <out.puffin> <referenced-data-file> <cardinality> [--roaring]

The `*-position-index.bin` files of Apache Iceberg are complete deletion vector blobs (the length,
the magic `D1 D3 39 64`, the portable 64-bit Roaring bitmap, the CRC-32). The `64map*.bin` files are
bare portable 64-bit Roaring bitmaps; with `--roaring` the length, the magic and the CRC-32 are added.
"""
import json
import struct
import sys
import zlib

payload_path, out_path, referenced_data_file, cardinality = sys.argv[1:5]
with open(payload_path, "rb") as f:
    blob = f.read()
if "--roaring" in sys.argv[5:]:
    body = bytes.fromhex("d1d33964") + blob
    blob = struct.pack(">I", len(body)) + body + struct.pack(">I", zlib.crc32(body))

footer = json.dumps({
    "blobs": [{
        "type": "deletion-vector-v1",
        "fields": [2147483645],
        "snapshot-id": -1,
        "sequence-number": -1,
        "offset": 4,
        "length": len(blob),
        "properties": {"referenced-data-file": referenced_data_file, "cardinality": cardinality},
    }],
    "properties": {},
}).encode()

with open(out_path, "wb") as f:
    f.write(b"PFA1" + blob + b"PFA1" + footer + struct.pack("<I", len(footer)) + b"\0\0\0\0" + b"PFA1")
