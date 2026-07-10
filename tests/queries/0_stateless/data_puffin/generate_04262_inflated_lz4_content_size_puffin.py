#!/usr/bin/env python3
"""Generate a puffin file with LZ4 footer advertising an inflated content size."""

from __future__ import annotations

import json
import struct
import subprocess
import sys
from pathlib import Path

try:
    import lz4.frame
    import xxhash
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "lz4", "xxhash", "-q"])
    import lz4.frame
    import xxhash

PUFFIN_MAGIC = b"PFA1"
INFLATED_CONTENT_SIZE = 0x40000000
OUTPUT_DIR = Path(__file__).parent
SOURCE = OUTPUT_DIR / "04077_deletion_vector.puffin"


def lz4_header_checksum(descriptor: bytes) -> int:
    return (xxhash.xxh32(descriptor, seed=0).intdigest() >> 8) & 0xFF


def add_inflated_content_size(compressed: bytes, fake_size: int) -> bytes:
    data = bytearray(compressed)
    flg = data[4] | 0x10
    bd = data[5]
    blocks = data[7:]
    content_size = struct.pack("<Q", fake_size)
    descriptor = bytes([flg, bd]) + content_size
    header_checksum = lz4_header_checksum(descriptor)
    return bytes(data[:4]) + descriptor + bytes([header_checksum]) + blocks


def extract_footer_json(puffin: bytes) -> bytes:
    footer_len = struct.unpack("<i", puffin[-12:-8])[0]
    footer_start = len(puffin) - 12 - footer_len
    return puffin[footer_start:footer_start + footer_len]


def build_puffin_file(blob: bytes, footer_json: bytes) -> bytes:
    footer_payload = add_inflated_content_size(lz4.frame.compress(footer_json), INFLATED_CONTENT_SIZE)
    footer_length = struct.pack("<i", len(footer_payload))
    flags = bytes([0x01, 0x00, 0x00, 0x00])

    return (
        PUFFIN_MAGIC
        + blob
        + PUFFIN_MAGIC
        + footer_payload
        + footer_length
        + flags
        + PUFFIN_MAGIC
    )


def main() -> None:
    source = SOURCE.read_bytes()
    blob_end = source.index(PUFFIN_MAGIC, 4)
    blob = source[4:blob_end]
    footer_json = extract_footer_json(source)
    puffin = build_puffin_file(blob, footer_json)

    output = OUTPUT_DIR / "04262_inflated_lz4_content_size.puffin"
    output.write_bytes(puffin)
    print(f"Wrote {output} ({len(puffin)} bytes), inflated content size {INFLATED_CONTENT_SIZE}")


if __name__ == "__main__":
    main()
