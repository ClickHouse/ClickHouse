"""Create Delta Lake tables with delta-rs (`deltalake`), then read every version of every table
back with delta-rs' DataFusion query engine.

Usage: python deltars_delta_writer.py <job.json>

Same job and result format as `spark_delta_writer.py`.
"""

import datetime
import decimal
import json
import os
import sys

import deltalake
import pyarrow as pa
import pyarrow.ipc as ipc
from deltalake import DeltaTable, QueryBuilder, write_deltalake


def simple(lo, hi):
    ids = list(range(lo, hi))
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "s": pa.array([f"v{i}" for i in ids], pa.string()),
        "p": pa.array([i % 4 for i in ids], pa.int32()),
    })


def all_types(lo, hi):
    ids = list(range(lo, hi))
    utc = datetime.timezone.utc
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "i": pa.array([i * 7 - 100 for i in ids], pa.int32()),
        "sh": pa.array([i * 3 - 50 for i in ids], pa.int16()),
        "by": pa.array([i % 256 - 128 for i in ids], pa.int8()),
        "f": pa.array([i / 4 for i in ids], pa.float32()),
        "d": pa.array([i * 1.5e10 for i in ids], pa.float64()),
        "dec": pa.array([decimal.Decimal(i * 325) / 100 for i in ids], pa.decimal128(10, 2)),
        "s": pa.array([None if i % 5 == 0 else f"s{i}" for i in ids], pa.string()),
        "bin": pa.array([f"b{i}".encode() for i in ids], pa.binary()),
        "b": pa.array([i % 2 == 0 for i in ids], pa.bool_()),
        "dt": pa.array([datetime.date(1970, 1, 1) + datetime.timedelta(days=i * 400 - 4000) for i in ids], pa.date32()),
        "ts": pa.array([datetime.datetime(2000, 1, 1, tzinfo=utc) + datetime.timedelta(microseconds=i * 86400123456) for i in ids],
                       pa.timestamp("us", "UTC")),
        "ts_ntz": pa.array([datetime.datetime(1999, 12, 31) + datetime.timedelta(microseconds=i * 3600000001) for i in ids],
                           pa.timestamp("us")),
        "st": pa.array([None if i % 7 == 0 else {"x": i, "y": str(i % 3)} for i in ids],
                       pa.struct([("x", pa.int32()), ("y", pa.string())])),
        "arr": pa.array([None if i % 6 == 0 else [i, None, i + 1] for i in ids], pa.list_(pa.int32())),
        "mp": pa.array([None if i % 4 == 0 else [("k", i), ("z", -i)] for i in ids], pa.map_(pa.string(), pa.int32())),
    })


def t_append_overwrite(path):
    write_deltalake(path, simple(0, 10))
    write_deltalake(path, simple(10, 20), mode="append")
    write_deltalake(path, simple(100, 105), mode="overwrite")
    write_deltalake(path, simple(105, 108), mode="append")


def t_partitioned(path):
    t = simple(0, 24)
    p2 = pa.array([None if i % 5 == 0 else ["a b", "x/y=z", "%", "ü"][i % 4] for i in range(24)], pa.string())
    t = t.append_column("p2", p2)
    write_deltalake(path, t, partition_by=["p", "p2"])
    DeltaTable(path).delete("p = 1")
    write_deltalake(path, simple(30, 34).append_column("p2", pa.array(["new"] * 4, pa.string())), mode="append")


def t_delete_update_merge(path):
    write_deltalake(path, simple(0, 30))
    DeltaTable(path).delete("id % 3 = 0")
    DeltaTable(path).update(updates={"s": "'updated'"}, predicate="id < 10")
    source = simple(25, 40)
    (DeltaTable(path).merge(source, predicate="t.id = s.id", source_alias="s", target_alias="t")
        .when_matched_update(updates={"s": "'merged'"})
        .when_not_matched_insert_all()
        .execute())


def t_schema_merge(path):
    write_deltalake(path, simple(0, 5))
    t = simple(5, 10).append_column("extra", pa.array([f"e{i}" for i in range(5)], pa.string()))
    write_deltalake(path, t, mode="append", schema_mode="merge")
    write_deltalake(path, simple(10, 12), mode="append", schema_mode="merge")


def t_checkpoint_optimize(path):
    for i in range(6):
        write_deltalake(path, simple(i * 10, i * 10 + 10), mode="append")
    DeltaTable(path).create_checkpoint()
    DeltaTable(path).optimize.compact()
    DeltaTable(path).delete("id >= 55")
    write_deltalake(path, simple(60, 62), mode="append")


def t_all_types(path):
    write_deltalake(path, all_types(0, 20))
    write_deltalake(path, all_types(20, 35), mode="append")


TABLES = {
    "append_overwrite": t_append_overwrite,
    "partitioned": t_partitioned,
    "delete_update_merge": t_delete_update_merge,
    "schema_merge": t_schema_merge,
    "checkpoint_optimize": t_checkpoint_optimize,
    "all_types": t_all_types,
}


def main():
    job = json.load(open(sys.argv[1]))
    results = []
    for base in job["tables"]:
        name = job.get("prefix", "") + base
        location = os.path.join(job["warehouse"], name)
        TABLES[base](location)
        versions = []
        for h in sorted(DeltaTable(location).history(), key=lambda h: h["version"]):
            dt = DeltaTable(location, version=h["version"])
            back = pa.table(QueryBuilder().register("t", dt).execute("SELECT * FROM t").read_all())
            with pa.OSFile(os.path.join(job["output_dir"], f"{name}.{h['version']}.arrow"), "wb") as f:
                w = ipc.new_file(f, back.schema)
                w.write_table(back)
                w.close()
            versions.append({"version": h["version"], "operation": h.get("operation", "")})
        p = DeltaTable(location).protocol()
        results.append({"name": name, "base": base, "location": location, "versions": versions,
                        "protocol": {"min_reader_version": p.min_reader_version, "min_writer_version": p.min_writer_version,
                                     "table_features": sorted((p.reader_features or []) + (p.writer_features or []))}})
    json.dump({"library": "deltalake", "version": deltalake.__version__, "results": results}, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
