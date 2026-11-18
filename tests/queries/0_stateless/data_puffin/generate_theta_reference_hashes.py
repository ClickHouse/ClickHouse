#!/usr/bin/env python3
"""Reference hashes of Theta sketches for 05813_puffin_theta_sketch_reference_hashes.

Values are encoded with the Iceberg binary single-value serialization and hashed as Apache DataSketches
`update(byte[])` does, independently of ClickHouse. For every column it prints the hashes that a compact
ordered sketch stores: sorted, as little-endian 64-bit words, in hex.
"""

from __future__ import annotations

import datetime
import struct
import subprocess
import sys
import uuid
from decimal import Context, Decimal

try:
    import mmh3
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mmh3", "-q"])
    import mmh3

DATASKETCHES_SEED = 9001
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def theta_hash(data: bytes) -> int:
    return mmh3.hash64(data, DATASKETCHES_SEED, signed=False)[0] >> 1


def micros(text: str) -> int:
    value = datetime.datetime.fromisoformat(text).replace(tzinfo=datetime.timezone.utc)
    delta = value - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def days(text: str) -> int:
    return (datetime.date.fromisoformat(text) - datetime.date(1970, 1, 1)).days


def decimal_bytes(value: str, scale: int) -> bytes:
    unscaled = int(Decimal(value).scaleb(scale, Context(prec=100)))
    size = 1
    while not -(1 << (8 * size - 1)) <= unscaled < (1 << (8 * size - 1)):
        size += 1
    return unscaled.to_bytes(size, "big", signed=True)


COLUMNS = {
    "b": [b"\x01", b"\x00"],
    "i8": [struct.pack("<i", v) for v in (-128, 5)],
    "u32": [struct.pack("<q", v) for v in (4294967295, 0)],
    "i64": [struct.pack("<q", v) for v in (1 << 40, -3)],
    "f32": [struct.pack("<f", v) for v in (1.5, -2.25)],
    "f64": [struct.pack("<d", v) for v in (0.1, 1e300)],
    "d": [struct.pack("<i", days(v)) for v in ("2020-01-01", "1970-01-02")],
    "d32": [struct.pack("<i", days(v)) for v in ("1950-01-01", "2100-12-31")],
    "dt": [struct.pack("<q", micros(v)) for v in ("2020-01-01 00:00:00", "2001-02-03 04:05:06")],
    "dt64_3": [struct.pack("<q", micros(v)) for v in ("2020-01-01 00:00:00.123", "1969-12-31 23:59:59.999")],
    "dt64_6": [struct.pack("<q", micros(v)) for v in ("2020-01-01 00:00:00.123456", "1900-01-01 00:00:00.000001")],
    "s": [v.encode("utf-8") for v in ("hello", "Привет")],
    "uuid": [uuid.UUID(v).bytes for v in ("00112233-4455-6677-8899-aabbccddeeff", "f0e1d2c3-b4a5-9687-7869-5a4b3c2d1e0f")],
    "dec32": [decimal_bytes(v, 2) for v in ("123.45", "-1.00")],
    "dec128": [decimal_bytes(v, 10) for v in ("1234567890123456789.0123456789", "-0.0000000001")],
}


def main() -> None:
    for name, values in COLUMNS.items():
        hashes = sorted(theta_hash(v) for v in values)
        print(f"{name}\t{len(hashes)}\t{b''.join(struct.pack('<Q', h) for h in hashes).hex().upper()}")


if __name__ == "__main__":
    main()
