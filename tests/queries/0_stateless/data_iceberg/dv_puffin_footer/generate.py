#!/usr/bin/env python3
"""Generates the tables of `05571_iceberg_deletion_vector_puffin_footer` from the Sirius tables in `../sirius`.

Each table is a copy of a Sirius table whose paths (metadata JSON, manifest list, manifests, Puffin footer) are moved
under this directory, with one change:

- `unknown_range`: `iceberg_v3_deletion_vector` whose deletion vector is one byte shorter than its Puffin blob.
- `same_name_swapped`: `iceberg_v3_dv_misbound` (the two vectors are swapped) with the data files renamed to
  `data/a/data.parquet` and `data/b/data.parquet`, so both vectors belong to files with the same name.
- `nested_suffix`: `iceberg_v3_dv_misbound` with the data files renamed to `data/a/data.parquet` and
  `data/b/data/a/data.parquet`, and both deletion vectors pointing at the blob of the second one, whose path ends with
  the path of the first.
- `mixed_blobs`: `iceberg_v3_deletion_vector` whose Puffin file also holds an `apache-datasketches-theta-v1` blob.
- `duplicate_range`: `iceberg_v3_deletion_vector` whose Puffin footer describes the deletion vector twice, first as
  the vector of another data file.

Usage: `python3 generate.py` (needs the `avro` package). The queries run with this directory as the current directory.
"""

import copy
import json
import shutil
import struct
import warnings
from pathlib import Path

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

HERE = Path(__file__).resolve().parent
SIRIUS_ROOT = HERE.parent / "sirius"
SIRIUS_TABLES = "test/cpp/integration/data"
PUFFIN_MAGIC = b"PFA1"

# The manifests declare `"logicalType": "map"` on arrays of key-value records, which the `avro` package ignores.
warnings.filterwarnings("ignore", module="avro")


def rewrite_strings(value, move):
    if isinstance(value, str):
        return move(value)
    if isinstance(value, dict):
        return {k: rewrite_strings(v, move) for k, v in value.items()}
    if isinstance(value, list):
        return [rewrite_strings(v, move) for v in value]
    return value


def read_puffin(path):
    data = path.read_bytes()
    (payload_size,) = struct.unpack("<i", data[-12:-8])
    payload_start = len(data) - 12 - payload_size
    assert data[:4] == PUFFIN_MAGIC and data[payload_start - 4 : payload_start] == PUFFIN_MAGIC and data[-4:] == PUFFIN_MAGIC
    return data[: payload_start - 4], json.loads(data[payload_start:-12])


def write_puffin(path, blob_region, footer):
    payload = json.dumps(footer, separators=(",", ":")).encode()
    path.write_bytes(blob_region + PUFFIN_MAGIC + payload + struct.pack("<i", len(payload)) + b"\x00\x00\x00\x00" + PUFFIN_MAGIC)


def copy_avro(src, dst, move, edit_record):
    with src.open("rb") as f:
        reader = DataFileReader(f, DatumReader())
        meta = dict(reader.meta)
        sync_marker = reader.sync_marker
        schema = reader.datum_reader.writers_schema
        records = [edit_record(rewrite_strings(record, move)) for record in reader]
    writer = DataFileWriter(dst.open("wb"), DatumWriter(), schema, codec=meta["avro.codec"].decode())
    for key, value in meta.items():
        writer.set_meta(key, value)
    writer.sync_marker = sync_marker
    for record in records:
        writer.append(record)
    writer.close()


def make_table(output_root, name, source, renames=None, edit_puffin=None, edit_manifest_entry=None):
    """Copies the Sirius table `source` to `output_root/name`, the paths in its metadata become `name/...`."""
    old_prefix = f"{SIRIUS_TABLES}/{source}"
    renames = renames or {}

    def move(path):
        if path == old_prefix:
            return name
        if not path.startswith(old_prefix + "/"):
            return path
        relative = path[len(old_prefix) + 1 :]
        return f"{name}/{renames.get(relative, relative)}"

    src_dir = SIRIUS_ROOT / old_prefix
    dst_dir = output_root / name
    shutil.rmtree(dst_dir, ignore_errors=True)

    for src in sorted((src_dir / "data").iterdir()):
        dst = output_root / move(f"{old_prefix}/data/{src.name}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.suffix == ".puffin":
            blob_region, footer = read_puffin(src)
            footer = rewrite_strings(footer, move)
            if edit_puffin:
                blob_region = edit_puffin(blob_region, footer["blobs"], name)
            write_puffin(dst, blob_region, footer)
        else:
            shutil.copyfile(src, dst)

    def edit_entry(record):
        data_file = record["data_file"]
        data_file["file_size_in_bytes"] = (output_root / data_file["file_path"]).stat().st_size
        if edit_manifest_entry:
            edit_manifest_entry(data_file)
        return record

    def edit_manifest_list_entry(record):
        record["manifest_length"] = (output_root / record["manifest_path"]).stat().st_size
        return record

    (dst_dir / "metadata").mkdir(parents=True)
    avro_files = sorted((src_dir / "metadata").glob("*.avro"))
    # Manifests first: the manifest list records their sizes.
    for src in sorted(avro_files, key=lambda p: p.name.startswith("snap-")):
        is_manifest_list = src.name.startswith("snap-")
        copy_avro(src, dst_dir / "metadata" / src.name, move, edit_manifest_list_entry if is_manifest_list else edit_entry)

    for src in sorted((src_dir / "metadata").glob("*.json")):
        metadata = rewrite_strings(json.loads(src.read_text()), move)
        (dst_dir / "metadata" / src.name).write_text(json.dumps(metadata, indent=2) + "\n")
    shutil.copyfile(src_dir / "metadata" / "version-hint.text", dst_dir / "metadata" / "version-hint.text")


def shorten_deletion_vector(data_file):
    if data_file["content"] == 1:
        data_file["content_size_in_bytes"] -= 1


def add_theta_sketch_blob(blob_region, blobs, name):
    sketch = b"\x01" * 8
    blobs.append(
        {
            "type": "apache-datasketches-theta-v1",
            "fields": [1],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": len(blob_region),
            "length": len(sketch),
            "properties": {"ndv": "1"},
        }
    )
    return blob_region + sketch


def describe_deletion_vector_twice(blob_region, blobs, name):
    impostor = copy.deepcopy(blobs[0])
    impostor["properties"]["referenced-data-file"] = f"{name}/data/other.parquet"
    blobs.insert(0, impostor)
    return blob_region


def point_deletion_vectors_at(table_dir, referenced_data_file):
    def edit(data_file):
        if data_file["content"] != 1:
            return
        (puffin,) = (table_dir / "data").glob("*.puffin")
        _, footer = read_puffin(puffin)
        blobs = [b for b in footer["blobs"] if b["properties"]["referenced-data-file"] == referenced_data_file]
        assert len(blobs) == 1
        data_file["content_offset"] = blobs[0]["offset"]
        data_file["content_size_in_bytes"] = blobs[0]["length"]

    return edit


def main():
    make_table(HERE, "unknown_range", "iceberg_v3_deletion_vector", edit_manifest_entry=shorten_deletion_vector)
    make_table(
        HERE,
        "same_name_swapped",
        "iceberg_v3_dv_misbound",
        renames={
            "data/00000-0-d7e8f9a0-0007-0007-0007-000000000001-00001.parquet": "data/a/data.parquet",
            "data/00001-0-d7e8f9a0-0007-0007-0007-000000000001-00001.parquet": "data/b/data.parquet",
        },
    )
    make_table(
        HERE,
        "nested_suffix",
        "iceberg_v3_dv_misbound",
        renames={
            "data/00000-0-d7e8f9a0-0007-0007-0007-000000000001-00001.parquet": "data/a/data.parquet",
            "data/00001-0-d7e8f9a0-0007-0007-0007-000000000001-00001.parquet": "data/b/data/a/data.parquet",
        },
        edit_manifest_entry=point_deletion_vectors_at(HERE / "nested_suffix", "nested_suffix/data/b/data/a/data.parquet"),
    )
    make_table(HERE, "mixed_blobs", "iceberg_v3_deletion_vector", edit_puffin=add_theta_sketch_blob)
    make_table(HERE, "duplicate_range", "iceberg_v3_deletion_vector", edit_puffin=describe_deletion_vector_twice)


if __name__ == "__main__":
    main()
