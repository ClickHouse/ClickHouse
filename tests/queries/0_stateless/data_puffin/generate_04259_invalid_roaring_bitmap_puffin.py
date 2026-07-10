#!/usr/bin/env python3
"""Generate a puffin file with a valid DV envelope but corrupt roaring bitmap bytes."""

from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path

PUFFIN_MAGIC = b"PFA1"
DELETION_VECTOR_MAGIC = bytes([0xD1, 0xD3, 0x39, 0x64])
OUTPUT_DIR = Path(__file__).parent


def serialize_corrupt_roaring_position_bitmap() -> bytes:
    # One bitmap at key 0, followed by invalid portable roaring bytes.
    return struct.pack("<qi", 1, 0) + b"\xFF\xFF\xFF\xFF\x00"


def wrap_deletion_vector_blob(vector: bytes) -> bytes:
    combined_length = len(DELETION_VECTOR_MAGIC) + len(vector)
    crc_input = DELETION_VECTOR_MAGIC + vector
    crc = zlib.crc32(crc_input) & 0xFFFFFFFF
    return struct.pack(">I", combined_length) + crc_input + struct.pack(">I", crc)


def build_puffin_file(blob: bytes) -> bytes:
    footer_payload = {
        "blobs": [
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": 1,
                "sequence-number": 1,
                "offset": 4,
                "length": len(blob),
                "properties": {},
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
    vector = serialize_corrupt_roaring_position_bitmap()
    blob = wrap_deletion_vector_blob(vector)
    puffin = build_puffin_file(blob)

    output = OUTPUT_DIR / "04259_invalid_roaring_bitmap.puffin"
    output.write_bytes(puffin)
    print(f"Wrote {output} ({len(puffin)} bytes), blob size {len(blob)}")


if __name__ == "__main__":
    main()
