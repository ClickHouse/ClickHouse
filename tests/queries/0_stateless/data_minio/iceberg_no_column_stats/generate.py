#!/usr/bin/env python3
"""Generate the stats-less Iceberg fixture for 04302_iceberg_read_optimization_no_column_stats.

Creates a 3-row table, strips every per-column statistic from the manifest so
ClickHouse's `DataFileMetaInfo::columns_info` is empty, and rewrites internal
paths to a stable `s3a://test/<name>` prefix. See README.md. Usage: generate.py <out_dir>.
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

import fastavro
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DoubleType

AVRO_RESERVED = {"avro.schema", "avro.codec"}

# Per-column statistics on a manifest data_file entry (all optional); clearing
# them all is what makes the manifest stats-less.
STAT_FIELDS = (
    "column_sizes",
    "value_counts",
    "null_value_counts",
    "nan_value_counts",
    "lower_bounds",
    "upper_bounds",
)


def deep_replace(obj, old, new):
    if isinstance(obj, str):
        return obj.replace(old, new)
    if isinstance(obj, dict):
        return {k: deep_replace(v, old, new) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deep_replace(v, old, new) for v in obj]
    return obj


def clear_stats(record):
    df = record.get("data_file")
    if isinstance(df, dict):
        for field in STAT_FIELDS:
            if field in df and df[field]:
                df[field] = []
    return record


def rewrite_avro(src: Path, dst: Path, old: str, new: str, strip_stats: bool):
    with open(src, "rb") as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
        meta = {k: v for k, v in reader.metadata.items() if k not in AVRO_RESERVED}
        records = [deep_replace(r, old, new) for r in reader]
    if strip_stats:
        records = [clear_stats(r) for r in records]
    with open(dst, "wb") as f:
        fastavro.writer(f, schema, records, metadata=meta)


def main(out_dir: str):
    work = Path(tempfile.mkdtemp(prefix="iceberg_gen_"))
    warehouse = work / "warehouse"
    warehouse.mkdir(parents=True)

    catalog = SqlCatalog(
        "gen",
        uri=f"sqlite:///{work}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("ns")

    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "value", DoubleType(), required=False),
    )

    # Reduces metrics, but pyiceberg still writes column_sizes (stripped below).
    table = catalog.create_table(
        "ns.no_stats",
        schema=schema,
        properties={"write.metadata.metrics.default": "none"},
    )

    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["alice", "bob", "carol"], type=pa.string()),
            "value": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
        }
    )
    table.append(data)

    table_location = Path(table.location().replace("file://", ""))
    old_prefix = table.location()                      # file:///tmp/.../ns.db/no_stats
    out = Path(out_dir)
    new_prefix = f"s3a://test/{out.name}"              # s3a://test/iceberg_no_column_stats

    if out.exists():
        shutil.rmtree(out)
    (out / "metadata").mkdir(parents=True)
    (out / "data").mkdir(parents=True)

    for f in (table_location / "data").rglob("*"):
        if f.is_file():
            rel = f.relative_to(table_location / "data")
            target = out / "data" / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, target)

    meta_dir = table_location / "metadata"

    # Keep only the latest metadata.json (the post-append snapshot). The empty
    # create-time version and the history logs that reference it aren't read.
    latest_json = max(
        (f for f in meta_dir.iterdir() if f.name.endswith(".metadata.json")),
        key=lambda f: int(f.name.split("-", 1)[0]),
    )
    meta = json.loads(latest_json.read_text().replace(old_prefix, new_prefix))
    meta["metadata-log"] = []
    meta["snapshot-log"] = []
    (out / "metadata" / latest_json.name).write_text(json.dumps(meta, separators=(",", ":")))

    for f in meta_dir.iterdir():
        if f.name.endswith(".avro"):
            # Only manifests carry data_file stats; the manifest list (snap-*) does not.
            strip = not f.name.startswith("snap-")
            rewrite_avro(f, out / "metadata" / f.name, old_prefix, new_prefix, strip)

    print(f"old prefix:  {old_prefix}")
    print(f"new prefix:  {new_prefix}")
    print(f"table uuid:  {table.metadata.table_uuid}")
    print(f"copied to:   {out}")
    shutil.rmtree(work)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <output_dir>")
    main(sys.argv[1])
