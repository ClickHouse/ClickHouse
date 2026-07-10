#!/usr/bin/env python3
"""Generate a puffin file with a sparse RoaringPositionBitmap at a large key."""

from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path

try:
    import pyroaring
except ImportError as exc:  # pragma: no cover - helper script
    raise SystemExit("pyroaring is required: pip install pyroaring") from exc

PUFFIN_MAGIC = b"PFA1"
DELETION_VECTOR_MAGIC = bytes([0xD1, 0xD3, 0x39, 0x64])
LARGE_KEY = 1_000_000
SUB_POSITION = 42
OUTPUT_DIR = Path(__file__).parent


def serialize_sparse_large_key_bitmap() -> bytes:
    bitmap = pyroaring.BitMap()
    bitmap.add(SUB_POSITION)
    return struct.pack("<qi", 1, LARGE_KEY) + bitmap.serialize()


def wrap_deletion_vector_blob(vector: bytes) -> bytes:
    combined_length = len(DELETION_VECTOR_MAGIC) + len(vector)
    crc_input = DELETION_VECTOR_MAGIC + vector
    crc = zlib.crc32(crc_input) & 0xFFFFFFFF
    return struct.pack(">I", combined_length) + crc_input + struct.pack(">I", crc)


def build_puffin_file(blob: bytes) -> bytes:
    position = (LARGE_KEY << 32) | SUB_POSITION
    footer_payload = {
        "blobs": [
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": 1,
                "sequence-number": 1,
                "offset": 4,
                "length": len(blob),
                "properties": {
                    "referenced-data-file": "/data/table/part-00000.parquet",
                    "cardinality": "1",
                },
            }
        ]
    }

    footer_json = json.dumps(footer_payload, separators=(", ", ": ")).encode("utf-8")
    footer_length = struct.pack("<i", len(footer_json))
    flags = b"\x00\x00\x00\x00"

    return (
        PUFFIN_MAGIC
        + blob
        + PUFFIN_MAGIC
        + footer_json
        + footer_length
        + flags
        + PUFFIN_MAGIC
    )


def main() -> None:
    vector = serialize_sparse_large_key_bitmap()
    blob = wrap_deletion_vector_blob(vector)
    puffin = build_puffin_file(blob)

    output = OUTPUT_DIR / "04260_sparse_large_key.puffin"
    output.write_bytes(puffin)
    position = (LARGE_KEY << 32) | SUB_POSITION
    print(f"Wrote {output} ({len(puffin)} bytes), position {position}")


if __name__ == "__main__":
    main()
