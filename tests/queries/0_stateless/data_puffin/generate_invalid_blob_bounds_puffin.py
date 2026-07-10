#!/usr/bin/env python3
"""Generate puffin files with invalid blob offset/length metadata."""

from __future__ import annotations

import json
import struct
from pathlib import Path

PUFFIN_MAGIC = b"PFA1"
OUTPUT_DIR = Path(__file__).parent
BLOB_PLACEHOLDER = b"\x00" * 58


def build_puffin_file(offset: int, length: int) -> bytes:
    footer_payload = {
        "blobs": [
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": 1,
                "sequence-number": 1,
                "offset": offset,
                "length": length,
                "properties": {},
            }
        ]
    }

    footer_json = json.dumps(footer_payload, separators=(", ", ": ")).encode("utf-8")
    footer_length = struct.pack("<i", len(footer_json))
    flags = b"\x00\x00\x00\x00"

    return (
        PUFFIN_MAGIC
        + BLOB_PLACEHOLDER
        + PUFFIN_MAGIC
        + footer_json
        + footer_length
        + flags
        + PUFFIN_MAGIC
    )


def main() -> None:
    cases = {
        "overflow_offset_length.puffin": (9223372036854775797, 20),
        "negative_offset.puffin": (-1, 10),
        "length_exceeds_file.puffin": (4, 10_000),
    }

    for name, (offset, length) in cases.items():
        path = OUTPUT_DIR / name
        path.write_bytes(build_puffin_file(offset, length))
        print(f"Wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
