#!/usr/bin/env python3
"""Generate synthetic puffin test fixtures for ClickHouse stateless tests."""

from __future__ import annotations

import json
import struct
import subprocess
import sys
import zlib
from pathlib import Path

try:
    import lz4.frame
    import xxhash
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "lz4", "xxhash", "-q"])
    import lz4.frame
    import xxhash

try:
    import pyroaring
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyroaring", "-q"])
    import pyroaring

OUTPUT_DIR = Path(__file__).parent
PUFFIN_MAGIC = b"PFA1"
DELETION_VECTOR_MAGIC = bytes([0xD1, 0xD3, 0x39, 0x64])
INFLATED_CONTENT_SIZE = 0x40000000
INVALID_KEY = 0x7FFFFFFF
LARGE_KEY = 1_000_000
SPARSE_SUB_POSITION = 42
BLOB_PLACEHOLDER = b"\x00" * 58
DEFAULT_REFERENCED_DATA_FILE = "/data/table/part-00000.parquet"


def default_dv_properties(cardinality: str = "0") -> dict[str, str]:
    return {
        "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
        "cardinality": cardinality,
    }


def wrap_deletion_vector_blob(vector: bytes) -> bytes:
    combined_length = len(DELETION_VECTOR_MAGIC) + len(vector)
    crc_input = DELETION_VECTOR_MAGIC + vector
    crc = zlib.crc32(crc_input) & 0xFFFFFFFF
    return struct.pack(">I", combined_length) + crc_input + struct.pack(">I", crc)


def footer_json_for_blob(blob: bytes, properties: dict[str, str] | None = None) -> bytes:
    payload = {
        "blobs": [
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": 1,
                "sequence-number": 1,
                "offset": 4,
                "length": len(blob),
                "properties": properties if properties is not None else default_dv_properties(),
            }
        ]
    }
    return json.dumps(payload, separators=(", ", ": ")).encode("utf-8")


def build_puffin_file(
    blob: bytes,
    footer_json: bytes,
    *,
    compressed: bool = False,
    lz4_declare_content_size: bool = True,
) -> bytes:
    return build_puffin_file_from_blobs(
        [blob],
        footer_json,
        compressed=compressed,
        lz4_declare_content_size=lz4_declare_content_size,
    )


def build_puffin_file_from_blobs(
    blobs: list[bytes],
    footer_json: bytes,
    *,
    compressed: bool = False,
    lz4_declare_content_size: bool = True,
) -> bytes:
    if compressed:
        # Puffin requires a single LZ4 frame; Content Size must be present for valid files.
        footer_payload = lz4.frame.compress(footer_json, store_size=lz4_declare_content_size)
        flags = bytes([0x01, 0x00, 0x00, 0x00])
    else:
        footer_payload = footer_json
        flags = b"\x00\x00\x00\x00"

    footer_length = struct.pack("<i", len(footer_payload))
    return PUFFIN_MAGIC + b"".join(blobs) + PUFFIN_MAGIC + footer_payload + footer_length + flags + PUFFIN_MAGIC


def extract_blob_and_footer_json(puffin: bytes) -> tuple[bytes, bytes]:
    blob_end = puffin.index(PUFFIN_MAGIC, 4)
    blob = puffin[4:blob_end]
    footer_len = struct.unpack("<i", puffin[-12:-8])[0]
    footer_start = len(puffin) - 12 - footer_len
    return blob, puffin[footer_start:footer_start + footer_len]


def lz4_header_checksum(descriptor: bytes) -> int:
    return (xxhash.xxh32(descriptor, seed=0).intdigest() >> 8) & 0xFF


def set_lz4_content_size(compressed: bytes, content_size: int) -> bytes:
    """Replace the Content Size field in an LZ4 frame that already declares it.

    FLG bit 0x08 is Content Size; the previous implementation wrongly used 0x10
    (Block Checksum), which produced frames that fail decompression.
    """
    data = bytearray(compressed)
    if len(data) < 15:
        raise ValueError("LZ4 frame is too short to contain a Content Size field")
    if (data[4] & 0x08) == 0:
        raise ValueError("LZ4 frame does not declare Content Size")
    struct.pack_into("<Q", data, 6, content_size)
    descriptor = bytes(data[4:14])
    data[14] = lz4_header_checksum(descriptor)
    return bytes(data)


def add_inflated_content_size(compressed: bytes, fake_size: int) -> bytes:
    return set_lz4_content_size(compressed, fake_size)


def write_fixture(name: str, content: bytes) -> None:
    path = OUTPUT_DIR / name
    path.write_bytes(content)
    print(f"Wrote {path} ({len(content)} bytes)")


def generate_compressed_footer(source: Path) -> None:
    puffin = source.read_bytes()
    blob, footer_json = extract_blob_and_footer_json(puffin)
    write_fixture("compressed_footer.puffin", build_puffin_file(blob, footer_json, compressed=True))


def generate_invalid_blob_bounds() -> None:
    cases = {
        "overflow_offset_length.puffin": (9223372036854775797, 20),
        "negative_offset.puffin": (-1, 10),
        "length_exceeds_file.puffin": (4, 10_000),
        # Offset/length stay within the total file size, but the blob would extend past the
        # end of the blob region (i.e. into the footer payload), which must also be rejected.
        "blob_overlaps_footer.puffin": (4, len(BLOB_PLACEHOLDER) + 8),
    }
    for name, (offset, length) in cases.items():
        footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
        payload = json.loads(footer_json.decode("utf-8"))
        payload["blobs"][0]["offset"] = offset
        payload["blobs"][0]["length"] = length
        write_fixture(name, build_puffin_file(BLOB_PLACEHOLDER, json.dumps(payload, separators=(", ", ": ")).encode("utf-8")))


def generate_invalid_roaring_bitmap() -> None:
    vector = struct.pack("<qi", 1, 0) + b"\xFF\xFF\xFF\xFF\x00"
    blob = wrap_deletion_vector_blob(vector)
    write_fixture("invalid_roaring_bitmap.puffin", build_puffin_file(blob, footer_json_for_blob(blob)))


def generate_invalid_bitmap_key() -> None:
    vector = struct.pack("<qi", 1, INVALID_KEY) + b"\x00" * 4
    blob = wrap_deletion_vector_blob(vector)
    write_fixture("invalid_bitmap_key.puffin", build_puffin_file(blob, footer_json_for_blob(blob)))


def generate_inflated_lz4_content_size(source: Path) -> None:
    puffin = source.read_bytes()
    blob, footer_json = extract_blob_and_footer_json(puffin)
    footer_payload = set_lz4_content_size(lz4.frame.compress(footer_json, store_size=True), INFLATED_CONTENT_SIZE)
    footer_length = struct.pack("<i", len(footer_payload))
    flags = bytes([0x01, 0x00, 0x00, 0x00])
    write_fixture(
        "inflated_lz4_content_size.puffin",
        PUFFIN_MAGIC + blob + PUFFIN_MAGIC + footer_payload + footer_length + flags + PUFFIN_MAGIC,
    )


def generate_missing_lz4_content_size() -> None:
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    write_fixture(
        "missing_lz4_content_size.puffin",
        build_puffin_file(
            BLOB_PLACEHOLDER,
            footer_json,
            compressed=True,
            lz4_declare_content_size=False,
        ),
    )


def generate_missing_required_fields() -> None:
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    payload = json.loads(footer_json.decode("utf-8"))
    blob_field_cases = {
        "missing_snapshot_id.puffin": "snapshot-id",
        "missing_sequence_number.puffin": "sequence-number",
        "missing_fields.puffin": "fields",
        "missing_type.puffin": "type",
        "missing_offset.puffin": "offset",
        "missing_length.puffin": "length",
    }
    for name, field in blob_field_cases.items():
        case_payload = json.loads(json.dumps(payload))
        del case_payload["blobs"][0][field]
        write_fixture(
            name,
            build_puffin_file(
                BLOB_PLACEHOLDER,
                json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
        )

    footer_field_cases = {
        "missing_blobs.puffin": {},
        "null_blobs.puffin": {"blobs": None},
        "null_blob_entry.puffin": {"blobs": [None]},
        "invalid_blob_entry.puffin": {"blobs": ["not-an-object"]},
    }
    for name, footer_payload in footer_field_cases.items():
        write_fixture(
            name,
            build_puffin_file(
                BLOB_PLACEHOLDER,
                json.dumps(footer_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
        )

    dv_property_cases = {
        "missing_properties.puffin": None,
        "missing_referenced_data_file.puffin": {"cardinality": "0"},
        "missing_cardinality.puffin": {"referenced-data-file": DEFAULT_REFERENCED_DATA_FILE},
        "invalid_properties_array.puffin": [],
        "invalid_properties_string.puffin": "not-an-object",
    }
    for name, properties in dv_property_cases.items():
        case_payload = json.loads(footer_json.decode("utf-8"))
        if properties is None:
            del case_payload["blobs"][0]["properties"]
        else:
            case_payload["blobs"][0]["properties"] = properties
        write_fixture(
            name,
            build_puffin_file(
                BLOB_PLACEHOLDER,
                json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
        )

    case_payload = json.loads(footer_json.decode("utf-8"))
    case_payload["blobs"][0]["compression-codec"] = "lz4"
    write_fixture(
        "dv_with_compression_codec.puffin",
        build_puffin_file(
            BLOB_PLACEHOLDER,
            json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
        ),
    )


def generate_cardinality_mismatch_large_bitmap() -> None:
    bitmap = pyroaring.BitMap([2, 5, 7, 100, 65536])
    vector = struct.pack("<qi", 1, 0) + bitmap.serialize()
    blob = wrap_deletion_vector_blob(vector)
    properties = {
        "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
        "cardinality": "1",
    }
    write_fixture(
        "cardinality_mismatch_large_bitmap.puffin",
        build_puffin_file(blob, footer_json_for_blob(blob, properties)),
    )


def generate_sparse_large_key() -> None:
    bitmap = pyroaring.BitMap()
    bitmap.add(SPARSE_SUB_POSITION)
    vector = struct.pack("<qi", 1, LARGE_KEY) + bitmap.serialize()
    blob = wrap_deletion_vector_blob(vector)
    properties = {
        "referenced-data-file": "/data/table/part-00000.parquet",
        "cardinality": "1",
    }
    write_fixture("sparse_large_key.puffin", build_puffin_file(blob, footer_json_for_blob(blob, properties)))


def generate_mixed_blob_types() -> None:
    theta_blob = b"\x00" * 16
    bitmap = pyroaring.BitMap([2, 5])
    vector = struct.pack("<qi", 1, 0) + bitmap.serialize()
    deletion_vector_blob = wrap_deletion_vector_blob(vector)
    theta_offset = len(PUFFIN_MAGIC)
    deletion_vector_offset = theta_offset + len(theta_blob)
    footer_json = json.dumps(
        {
            "blobs": [
                {
                    "type": "apache-datasketches-theta-v1",
                    "fields": [1],
                    "snapshot-id": -1,
                    "sequence-number": -1,
                    "offset": theta_offset,
                    "length": len(theta_blob),
                    "properties": {},
                },
                {
                    "type": "deletion-vector-v1",
                    "fields": [],
                    "snapshot-id": -1,
                    "sequence-number": -1,
                    "offset": deletion_vector_offset,
                    "length": len(deletion_vector_blob),
                    "properties": {
                        "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
                        "cardinality": "2",
                    },
                },
            ]
        },
        separators=(", ", ": "),
    ).encode("utf-8")
    write_fixture(
        "mixed_blob_types.puffin",
        build_puffin_file_from_blobs([theta_blob, deletion_vector_blob], footer_json),
    )


def generate_invalid_non_dv_properties() -> None:
    theta_blob = b"\x00" * 16
    bitmap = pyroaring.BitMap([2, 5])
    vector = struct.pack("<qi", 1, 0) + bitmap.serialize()
    deletion_vector_blob = wrap_deletion_vector_blob(vector)
    theta_offset = len(PUFFIN_MAGIC)
    deletion_vector_offset = theta_offset + len(theta_blob)
    footer_template = {
        "blobs": [
            {
                "type": "apache-datasketches-theta-v1",
                "fields": [1],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": theta_offset,
                "length": len(theta_blob),
                "properties": {},
            },
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": deletion_vector_offset,
                "length": len(deletion_vector_blob),
                "properties": {
                    "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
                    "cardinality": "2",
                },
            },
        ]
    }
    non_dv_property_cases = {
        "invalid_non_dv_properties_array.puffin": [],
        "invalid_non_dv_properties_string.puffin": "not-an-object",
    }
    for name, properties in non_dv_property_cases.items():
        case_payload = json.loads(json.dumps(footer_template))
        case_payload["blobs"][0]["properties"] = properties
        write_fixture(
            name,
            build_puffin_file_from_blobs(
                [theta_blob, deletion_vector_blob],
                json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
        )


def main() -> None:
    spark_fixture = OUTPUT_DIR / "spark_deletion_vector.puffin"
    if not spark_fixture.exists():
        raise SystemExit(f"Missing {spark_fixture}; run generate_spark_puffin.py first")

    generate_compressed_footer(spark_fixture)
    generate_invalid_blob_bounds()
    generate_invalid_roaring_bitmap()
    generate_invalid_bitmap_key()
    generate_inflated_lz4_content_size(spark_fixture)
    generate_missing_lz4_content_size()
    generate_missing_required_fields()
    generate_mixed_blob_types()
    generate_invalid_non_dv_properties()
    generate_cardinality_mismatch_large_bitmap()
    generate_sparse_large_key()


if __name__ == "__main__":
    main()
