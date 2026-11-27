"""Create Iceberg tables with PyIceberg (SQL catalog: metadata files named
`<sequence>-<uuid>.metadata.json` and no `version-hint.text`), then read every snapshot back.

Usage: python pyiceberg_writer.py <job.json>
"""

import datetime
import decimal
import json
import os
import sys
import uuid

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pyiceberg
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform, HourTransform, IdentityTransform, MonthTransform, TruncateTransform, YearTransform
from pyiceberg.types import (
    BinaryType, BooleanType, DateType, DecimalType, DoubleType, FixedType, FloatType, IntegerType, ListType, LongType, MapType,
    NestedField, StringType, StructType, TimestampType, TimestamptzType, TimeType, UUIDType,
)

EPOCH = datetime.datetime(1970, 1, 1)


def all_types_schema():
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "i", IntegerType()),
        NestedField(3, "f", FloatType()),
        NestedField(4, "d", DoubleType()),
        NestedField(5, "dec", DecimalType(9, 3)),
        NestedField(6, "dec38", DecimalType(38, 10)),
        NestedField(7, "s", StringType()),
        NestedField(8, "bin", BinaryType()),
        NestedField(9, "fx", FixedType(5)),
        NestedField(10, "b", BooleanType()),
        NestedField(11, "dt", DateType()),
        NestedField(12, "tm", TimeType()),
        NestedField(13, "ts", TimestampType()),
        NestedField(14, "tstz", TimestamptzType()),
        NestedField(15, "u", UUIDType()),
        NestedField(16, "st", StructType(NestedField(17, "x", IntegerType()), NestedField(18, "y", StringType()))),
        NestedField(19, "arr", ListType(20, LongType(), element_required=False)),
        NestedField(21, "mp", MapType(22, StringType(), 23, IntegerType(), value_required=False)),
    )


def all_types_rows(lo, hi):
    out = []
    for i in range(lo, hi):
        out.append({
            "id": i, "i": None if i % 5 == 0 else i * 7 - 50, "f": i / 8, "d": i * 1.25e12, "dec": decimal.Decimal(i * 1234).scaleb(-3),
            "dec38": decimal.Decimal(i * 10**20 + 7).scaleb(-10), "s": None if i % 4 == 0 else f"str{i}", "bin": f"bin{i}".encode(),
            "fx": f"{i:05d}".encode(), "b": i % 3 == 0, "dt": datetime.date(1900, 1, 1) + datetime.timedelta(days=i * 997),
            "tm": datetime.time(i % 24, i % 60, i % 60, i * 1000 % 1000000), "ts": EPOCH + datetime.timedelta(seconds=i * 86399, microseconds=i),
            "tstz": (EPOCH + datetime.timedelta(seconds=i * 3601 - 10**8)).replace(tzinfo=datetime.timezone.utc),
            "u": uuid.UUID(int=i * 0x1234567890ABCDEF), "st": None if i % 6 == 0 else {"x": i, "y": str(i % 2)},
            "arr": None if i % 7 == 0 else [i, None, -i], "mp": None if i % 3 == 1 else {"a": i, "b": None},
        })
    return out


def simple_schema():
    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "s", StringType()),
        NestedField(3, "p", IntegerType()),
        NestedField(4, "ts", TimestampType()),
        NestedField(5, "d", DateType()),
    )


def simple_rows(lo, hi):
    return pa.table({
        "id": pa.array(range(lo, hi), pa.int64()),
        "s": pa.array([f"val{i % 9}" for i in range(lo, hi)]),
        "p": pa.array([i % 4 for i in range(lo, hi)], pa.int32()),
        "ts": pa.array([EPOCH + datetime.timedelta(hours=i * 7) for i in range(lo, hi)], pa.timestamp("us")),
        "d": pa.array([datetime.date(2020, 1, 1) + datetime.timedelta(days=i * 13) for i in range(lo, hi)], pa.date32()),
    })


def build(catalog, name, builder, fmt):
    ident = f"db.{name}"
    props = {"format-version": str(fmt)}
    if builder == "py_all_types":
        schema = all_types_schema()
        t = catalog.create_table(ident, schema=schema, properties=props)
        arrow_schema = t.schema().as_arrow()
        t.append(pa.Table.from_pylist(all_types_rows(0, 20), schema=arrow_schema))
        t.append(pa.Table.from_pylist(all_types_rows(20, 30), schema=arrow_schema))
    elif builder == "py_transforms":
        spec = PartitionSpec(
            PartitionField(1, 1000, BucketTransform(4), "id_bucket"),
            PartitionField(2, 1001, TruncateTransform(4), "s_trunc"),
            PartitionField(3, 1002, IdentityTransform(), "p"),
            PartitionField(4, 1003, HourTransform(), "ts_hour"),
            PartitionField(5, 1004, YearTransform(), "d_year"),
        )
        t = catalog.create_table(ident, schema=simple_schema(), partition_spec=spec, properties=props)
        t.append(simple_rows(0, 40))
    elif builder == "py_month_day":
        spec = PartitionSpec(PartitionField(4, 1000, DayTransform(), "ts_day"), PartitionField(5, 1001, MonthTransform(), "d_month"))
        t = catalog.create_table(ident, schema=simple_schema(), partition_spec=spec, properties=props)
        t.append(simple_rows(0, 30))
    elif builder == "py_overwrite_delete":
        t = catalog.create_table(ident, schema=simple_schema(), partition_spec=PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "p")), properties=props)
        t.append(simple_rows(0, 30))
        t.append(simple_rows(30, 50))
        t.delete(LessThan("id", 10))
        t.overwrite(simple_rows(100, 105), overwrite_filter=EqualTo("p", 1))
        t.delete(GreaterThanOrEqual("id", 45))
    elif builder == "py_schema_evolution":
        t = catalog.create_table(ident, schema=simple_schema(), properties=props)
        t.append(simple_rows(0, 10))
        with t.update_schema() as u:
            u.add_column("added", StringType())
            u.rename_column("s", "s_renamed")
            u.update_column("p", field_type=LongType())
        rows = simple_rows(10, 20).rename_columns(["id", "s_renamed", "p", "ts", "d"])
        rows = rows.set_column(2, "p", rows.column("p").cast(pa.int64())).append_column("added", pa.array([f"a{i}" for i in range(10)]))
        t.append(rows)
        with t.update_schema() as u:
            u.delete_column("ts")
        t.append(rows.drop_columns(["ts"]))
    elif builder == "py_name_mapping":
        # Parquet files without field ids, registered with `add_files`: readers must resolve columns by the name mapping.
        t = catalog.create_table(ident, schema=simple_schema(), properties=props)
        files = []
        for k, (lo, hi) in enumerate([(0, 15), (15, 25)]):
            path = os.path.join(t.location().removeprefix("file://"), "data", f"external_{k}.parquet")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            pq.write_table(simple_rows(lo, hi), path)
            files.append("file://" + path)
        t.add_files(files)
    else:
        raise ValueError(name)
    return catalog.load_table(ident)


def main():
    job = json.load(open(sys.argv[1]))
    os.makedirs(job["warehouse"], exist_ok=True)
    catalog = SqlCatalog("corpus", uri=f"sqlite:///{job['warehouse']}/catalog.db", warehouse=f"file://{job['warehouse']}")
    catalog.create_namespace_if_not_exists("db")
    results = []
    for spec in job["tables"]:
        name, fmt = spec["name"], spec["format_version"]
        if catalog.table_exists(f"db.{name}"):
            catalog.purge_table(f"db.{name}")
        t = build(catalog, name, spec.get("builder", name), fmt)
        snapshots = []
        for s in t.snapshots():
            data = t.scan(snapshot_id=s.snapshot_id).to_arrow()
            with pa.OSFile(os.path.join(job["output_dir"], f"{name}.{s.snapshot_id}.arrow"), "wb") as f:
                w = ipc.new_file(f, data.schema)
                w.write_table(data)
                w.close()
            snapshots.append({"snapshot_id": s.snapshot_id, "timestamp_ms": s.timestamp_ms, "operation": str(s.summary.operation.value) if s.summary else None})
        results.append({"name": name, "location": t.location().removeprefix("file://"), "format_version": fmt, "snapshots": snapshots})
    json.dump({"library": "pyiceberg", "version": pyiceberg.__version__, "results": results}, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
