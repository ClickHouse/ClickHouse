#!/usr/bin/env python3
"""Generate a puffin file with LZ4-compressed footer payload."""

from __future__ import annotations

import json
import struct
import subprocess
import sys
from pathlib import Path

try:
    import lz4.frame
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "lz4", "-q"])
    import lz4.frame

PUFFIN_MAGIC = b"PFA1"
OUTPUT = Path(__file__).with_name("04258_compressed_footer.puffin")
SOURCE = Path(__file__).with_name("04077_deletion_vector.puffin")


def extract_footer_json(puffin: bytes) -> bytes:
    footer_len = struct.unpack("<i", puffin[-12:-8])[0]
    footer_start = len(puffin) - 12 - footer_len
    return puffin[footer_start:footer_start + footer_len]


def build_puffin_file(blob: bytes, footer_json: bytes) -> bytes:
    footer_payload = lz4.frame.compress(footer_json)
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
    OUTPUT.write_bytes(puffin)
    print(f"Wrote {OUTPUT} ({len(puffin)} bytes), compressed footer {len(puffin) - len(source)} bytes delta")


if __name__ == "__main__":
    main()
