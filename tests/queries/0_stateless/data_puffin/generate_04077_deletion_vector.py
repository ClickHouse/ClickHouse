#!/usr/bin/env python3
"""Generate a spec-compliant Puffin file with one deletion-vector-v1 blob."""

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
POSITIONS = [2, 5, 7, 100, 65536]


def serialize_roaring_position_bitmap(positions: list[int]) -> bytes:
  bitmaps: dict[int, pyroaring.BitMap] = {}
  for position in positions:
    key = position >> 32
    sub_position = position & 0xFFFFFFFF
    bitmaps.setdefault(key, pyroaring.BitMap()).add(sub_position)

  keys = sorted(bitmaps)
  result = bytearray()
  result += struct.pack("<q", len(keys))
  for key in keys:
    result += struct.pack("<i", key)
    result += bitmaps[key].serialize()
  return bytes(result)


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
        "properties": {
          "referenced-data-file": "/data/table/part-00000.parquet",
          "cardinality": str(len(POSITIONS)),
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
  vector = serialize_roaring_position_bitmap(POSITIONS)
  blob = wrap_deletion_vector_blob(vector)
  puffin = build_puffin_file(blob)

  output = Path(__file__).with_name("04077_deletion_vector.puffin")
  output.write_bytes(puffin)
  print(f"Wrote {output} ({len(puffin)} bytes), blob size {len(blob)}")


if __name__ == "__main__":
  main()
