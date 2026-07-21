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
# Must stay in sync with PUFFIN_FOOTER_LZ4_MAX_DECOMPRESSED_SIZE in PuffinBlockInputFormat.cpp.
FOOTER_LZ4_ABSOLUTE_DECOMPRESSED_LIMIT = 16 * 1024 * 1024
FOOTER_LZ4_MAX_RATIO = 255
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
                "snapshot-id": -1,
                "sequence-number": -1,
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


def generate_lz4_content_size_within_ratio_over_absolute_cap(source: Path) -> None:
    """contentSize passes size*255 but exceeds the absolute decompressed-footer cap.

    Pads the compressed footer payload so the ratio guard would allow the forged size;
    the parser must reject on the absolute ceiling before allocating.
    """
    puffin = source.read_bytes()
    blob, footer_json = extract_blob_and_footer_json(puffin)
    forged_content_size = FOOTER_LZ4_ABSOLUTE_DECOMPRESSED_LIMIT + 1
    min_payload_for_ratio = (forged_content_size + FOOTER_LZ4_MAX_RATIO - 1) // FOOTER_LZ4_MAX_RATIO
    footer_payload = bytearray(
        set_lz4_content_size(lz4.frame.compress(footer_json, store_size=True), forged_content_size)
    )
    if len(footer_payload) < min_payload_for_ratio:
        footer_payload.extend(b"\x00" * (min_payload_for_ratio - len(footer_payload)))
    footer_length = struct.pack("<i", len(footer_payload))
    flags = bytes([0x01, 0x00, 0x00, 0x00])
    write_fixture(
        "lz4_content_size_over_absolute_cap.puffin",
        PUFFIN_MAGIC + blob + PUFFIN_MAGIC + bytes(footer_payload) + footer_length + flags + PUFFIN_MAGIC,
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


def generate_lz4_trailing_bytes() -> None:
    """Valid single LZ4 frame plus trailing garbage; FooterPayloadSize includes the junk."""
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    footer_payload = lz4.frame.compress(footer_json, store_size=True) + b"GARBAGE"
    footer_length = struct.pack("<i", len(footer_payload))
    flags = bytes([0x01, 0x00, 0x00, 0x00])
    write_fixture(
        "lz4_trailing_bytes.puffin",
        PUFFIN_MAGIC + BLOB_PLACEHOLDER + PUFFIN_MAGIC + footer_payload + footer_length + flags + PUFFIN_MAGIC,
    )


def generate_incomplete_lz4_footer() -> None:
    """Truncated LZ4 frame: header parses with content size, body is incomplete (must not hang)."""
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    full_frame = lz4.frame.compress(footer_json, store_size=True)
    # Keep enough bytes for LZ4F_getFrameInfo to succeed, but truncate the body.
    footer_payload = full_frame[:19]
    footer_length = struct.pack("<i", len(footer_payload))
    flags = bytes([0x01, 0x00, 0x00, 0x00])
    write_fixture(
        "incomplete_lz4_footer.puffin",
        PUFFIN_MAGIC + BLOB_PLACEHOLDER + PUFFIN_MAGIC + footer_payload + footer_length + flags + PUFFIN_MAGIC,
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


def generate_invalid_property_value_types() -> None:
    """Property maps must have string values; non-strings must fail with BAD_ARGUMENTS."""
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    base = json.loads(footer_json.decode("utf-8"))
    cases = {
        # Extra key keeps required DV strings valid so the failure is the value type.
        "invalid_property_number.puffin": {**default_dv_properties(), "ndv": 5},
        "invalid_property_bool.puffin": {**default_dv_properties(), "flag": True},
        "invalid_property_null.puffin": {**default_dv_properties(), "x": None},
        "invalid_property_object.puffin": {**default_dv_properties(), "nested": {"a": 1}},
        # Required key itself typed wrong must also reject as non-string.
        "invalid_property_cardinality_number.puffin": {
            "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
            "cardinality": 5,
        },
    }
    for name, properties in cases.items():
        case_payload = json.loads(json.dumps(base))
        case_payload["blobs"][0]["properties"] = properties
        write_fixture(
            name,
            build_puffin_file(
                BLOB_PLACEHOLDER,
                json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
        )


def generate_invalid_integer_fields() -> None:
    """BlobMetadata integer fields must be JSON integers, not floats, strings, or booleans.

    Poco reports JSON booleans as integers (`std::numeric_limits<bool>::is_integer`), so
    boolean cases must be rejected explicitly in the reader.
    """
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    base = json.loads(footer_json.decode("utf-8"))
    cases = {
        "float_offset.puffin": ("offset", 5.1),
        "float_length.puffin": ("length", 58.9),
        "float_snapshot_id.puffin": ("snapshot-id", -1.5),
        "float_sequence_number.puffin": ("sequence-number", 1.2),
        "float_fields_element.puffin": ("fields", [1.9]),
        "string_offset.puffin": ("offset", "4"),
        "bool_offset.puffin": ("offset", True),
        "bool_snapshot_id.puffin": ("snapshot-id", False),
        "bool_fields_element.puffin": ("fields", [True]),
        "fields_element_out_of_int32_range.puffin": ("fields", [2**40]),
        # Poco stores integers that fail signed Int64 parse as UInt64 (2**63 wraps in tryParse64).
        "offset_out_of_int64_range.puffin": ("offset", 2**64 - 1),
        "fields_element_out_of_int64_range.puffin": ("fields", [2**64 - 1]),
    }
    for name, (field, value) in cases.items():
        case_payload = json.loads(json.dumps(base))
        case_payload["blobs"][0][field] = value
        write_fixture(
            name,
            build_puffin_file(
                BLOB_PLACEHOLDER,
                json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
        )


def generate_invalid_string_fields() -> None:
    """BlobMetadata type / compression-codec must be JSON strings."""
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    base = json.loads(footer_json.decode("utf-8"))

    type_cases = {
        "type_number.puffin": 123,
        "type_bool.puffin": True,
    }
    for name, type_value in type_cases.items():
        case_payload = json.loads(json.dumps(base))
        case_payload["blobs"][0]["type"] = type_value
        write_fixture(
            name,
            build_puffin_file(
                BLOB_PLACEHOLDER,
                json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
        )

    # Non-DV blob so DV omit-codec logic does not hide the type error.
    theta_cases = {
        "compression_codec_number.puffin": 1,
        "compression_codec_bool.puffin": True,
    }
    for name, codec_value in theta_cases.items():
        case_payload = {
            "blobs": [
                {
                    "type": "apache-datasketches-theta-v1",
                    "fields": [],
                    "snapshot-id": -1,
                    "sequence-number": -1,
                    "offset": 4,
                    "length": len(BLOB_PLACEHOLDER),
                    "compression-codec": codec_value,
                }
            ]
        }
        write_fixture(
            name,
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


def generate_dense_range_100k() -> None:
    """RLE-dense roaring: tiny on-disk blob, 100k consecutive positions — must remain readable."""
    bitmap = pyroaring.BitMap()
    bitmap.add_range(0, 100_000)
    vector = struct.pack("<qi", 1, 0) + bitmap.serialize()
    blob = wrap_deletion_vector_blob(vector)
    properties = {
        "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
        "cardinality": str(len(bitmap)),
    }
    write_fixture(
        "dense_range_100k.puffin",
        build_puffin_file(blob, footer_json_for_blob(blob, properties)),
    )


def generate_cardinality_exceeds_materialization_limit() -> None:
    """Declared cardinality above the absolute materialization ceiling must be rejected early."""
    bitmap = pyroaring.BitMap([0])
    vector = struct.pack("<qi", 1, 0) + bitmap.serialize()
    blob = wrap_deletion_vector_blob(vector)
    properties = {
        "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
        "cardinality": "100000001",
    }
    write_fixture(
        "cardinality_exceeds_materialization_limit.puffin",
        build_puffin_file(blob, footer_json_for_blob(blob, properties)),
    )


def generate_invalid_cardinality_strings() -> None:
    """cardinality must parse as UInt64; invalid strings must fail with BAD_ARGUMENTS."""
    bitmap = pyroaring.BitMap([2, 5])
    vector = struct.pack("<qi", 1, 0) + bitmap.serialize()
    blob = wrap_deletion_vector_blob(vector)
    cases = {
        "invalid_cardinality_non_numeric.puffin": "not-a-number",
        "invalid_cardinality_negative.puffin": "-1",
    }
    for name, cardinality in cases.items():
        properties = {
            "referenced-data-file": DEFAULT_REFERENCED_DATA_FILE,
            "cardinality": cardinality,
        }
        write_fixture(name, build_puffin_file(blob, footer_json_for_blob(blob, properties)))


def generate_invalid_dv_snapshot_sequence() -> None:
    """Iceberg requires deletion-vector-v1 snapshot-id and sequence-number to be -1."""
    footer_json = footer_json_for_blob(BLOB_PLACEHOLDER)
    base = json.loads(footer_json.decode("utf-8"))
    cases = {
        "dv_nonzero_snapshot_id.puffin": ("snapshot-id", 1),
        "dv_nonzero_sequence_number.puffin": ("sequence-number", 1),
    }
    for name, (field, value) in cases.items():
        case_payload = json.loads(json.dumps(base))
        case_payload["blobs"][0][field] = value
        write_fixture(
            name,
            build_puffin_file(
                BLOB_PLACEHOLDER,
                json.dumps(case_payload, separators=(", ", ": ")).encode("utf-8"),
            ),
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


def write_raw_footer_fixture(name: str, footer_payload: bytes) -> None:
    flags = b"\x00\x00\x00\x00"
    footer_length = struct.pack("<i", len(footer_payload))
    write_fixture(
        name,
        PUFFIN_MAGIC + BLOB_PLACEHOLDER + PUFFIN_MAGIC + footer_payload + footer_length + flags + PUFFIN_MAGIC,
    )


def generate_invalid_footer_root() -> None:
    """FileMetadata must be a JSON object.

    Poco's RFC 4627 parser accepts array as a top-level value, so this exercises the
    Object::Ptr guard. String/number roots fail earlier inside the parser itself.
    """
    write_raw_footer_fixture("footer_root_array.puffin", b"[1, 2, 3]")


def generate_invalid_file_metadata_properties() -> None:
    """Optional FileMetadata.properties must be a JSON object with string values."""
    bitmap = pyroaring.BitMap([2, 5])
    vector = struct.pack("<qi", 1, 0) + bitmap.serialize()
    blob = wrap_deletion_vector_blob(vector)
    base = {
        "blobs": [
            {
                "type": "deletion-vector-v1",
                "fields": [],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": 4,
                "length": len(blob),
                "properties": default_dv_properties(cardinality="2"),
            }
        ]
    }

    cases: dict[str, object] = {
        "invalid_file_properties_array.puffin": [],
        "invalid_file_properties_string.puffin": "not-an-object",
        "invalid_file_property_number.puffin": {"created-by": "ok", "ndv": 5},
    }
    for name, properties in cases.items():
        payload = json.loads(json.dumps(base))
        payload["properties"] = properties
        write_fixture(name, build_puffin_file(blob, json.dumps(payload, separators=(", ", ": ")).encode("utf-8")))

    ok_payload = json.loads(json.dumps(base))
    ok_payload["properties"] = {"created-by": "ClickHouse test"}
    write_fixture(
        "file_properties_ok.puffin",
        build_puffin_file(blob, json.dumps(ok_payload, separators=(", ", ": ")).encode("utf-8")),
    )


def generate_unparseable_footer_json() -> None:
    """Malformed JSON / oversize integers must fail with BAD_ARGUMENTS, not STD_EXCEPTION."""
    write_raw_footer_fixture("malformed_footer_json.puffin", b"{")
    # Larger than UInt64::max; Poco NumberParser throws SyntaxException.
    write_raw_footer_fixture(
        "footer_integer_overflow.puffin",
        b'{"blobs": [{"type": "apache-datasketches-theta-v1", "fields": [], '
        b'"snapshot-id": 99999999999999999999999999999999, "sequence-number": 1, '
        b'"offset": 4, "length": 4, "properties": {}}]}',
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
    generate_lz4_content_size_within_ratio_over_absolute_cap(spark_fixture)
    generate_missing_lz4_content_size()
    generate_lz4_trailing_bytes()
    generate_incomplete_lz4_footer()
    generate_missing_required_fields()
    generate_invalid_property_value_types()
    generate_invalid_integer_fields()
    generate_invalid_string_fields()
    generate_invalid_footer_root()
    generate_invalid_file_metadata_properties()
    generate_unparseable_footer_json()
    generate_mixed_blob_types()
    generate_invalid_non_dv_properties()

    generate_cardinality_mismatch_large_bitmap()
    generate_dense_range_100k()
    generate_cardinality_exceeds_materialization_limit()
    generate_invalid_cardinality_strings()
    generate_invalid_dv_snapshot_sequence()
    generate_sparse_large_key()


if __name__ == "__main__":
    main()
