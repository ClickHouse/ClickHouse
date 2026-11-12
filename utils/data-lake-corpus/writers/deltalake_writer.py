"""Write Parquet files with delta-rs (`deltalake`, the Rust `parquet` crate of arrow-rs).

Usage: python deltalake_writer.py <job.json>

Each job writes a one-file Delta table and keeps its data file. The readback goes through
delta-rs' DataFusion query engine, which reads Parquet with arrow-rs as well.
"""

import glob
import json
import os
import shutil
import sys

import deltalake
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from deltalake import BloomFilterProperties, ColumnProperties, DeltaTable, QueryBuilder, WriterProperties, write_deltalake


def main():
    job = json.load(open(sys.argv[1]))
    results = []
    for j in job["jobs"]:
        with pa.memory_map(os.path.join(job["input_dir"], j["input"])) as f:
            table = ipc.open_file(f).read_all()
        opts = dict(j.get("options", {}))
        bloom = opts.pop("_bloom", None)
        columns = {c: ColumnProperties(**p) for c, p in opts.pop("_columns", {}).items()}
        if bloom:
            columns.update({c: ColumnProperties(bloom_filter_properties=BloomFilterProperties(set_bloom_filter_enabled=True, fpp=0.01, ndv=1000)) for c in bloom})
        if columns:
            opts["column_properties"] = columns
        if "_default" in opts:
            opts["default_column_properties"] = ColumnProperties(**opts.pop("_default"))
        table_dir = os.path.join(job["output_dir"], j["name"] + ".delta")
        write_deltalake(table_dir, table, writer_properties=WriterProperties(**opts))
        parts = glob.glob(os.path.join(table_dir, "*.parquet"))
        assert len(parts) == 1, parts
        out = os.path.join(job["output_dir"], j["name"] + ".parquet")
        shutil.copyfile(parts[0], out)
        dt = DeltaTable(table_dir)
        back = pa.table(QueryBuilder().register("t", dt).execute("SELECT * FROM t").read_all())
        shutil.rmtree(table_dir)
        with pa.OSFile(os.path.join(job["output_dir"], j["name"] + ".arrow"), "wb") as f:
            w = ipc.new_file(f, back.schema)
            w.write_table(back)
            w.close()
        md = pq.ParquetFile(out).metadata
        results.append({"name": j["name"], "created_by": md.created_by, "num_row_groups": md.num_row_groups})
    json.dump({"library": "deltalake", "version": deltalake.__version__, "results": results}, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
