"""Write Parquet files with polars (its own Rust writer, `polars-parquet`), read back with polars.

Usage: python polars_writer.py <job.json>
"""

import json
import os
import sys

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


def main():
    job = json.load(open(sys.argv[1]))
    results = []
    for j in job["jobs"]:
        with pa.memory_map(os.path.join(job["input_dir"], j["input"])) as f:
            table = ipc.open_file(f).read_all()
        df = pl.from_arrow(table)
        out = os.path.join(job["output_dir"], j["name"] + ".parquet")
        df.write_parquet(out, **j.get("options", {}))
        back = pl.read_parquet(out).to_arrow()
        with pa.OSFile(os.path.join(job["output_dir"], j["name"] + ".arrow"), "wb") as f:
            w = ipc.new_file(f, back.schema)
            w.write_table(back)
            w.close()
        md = pq.ParquetFile(out).metadata
        results.append({"name": j["name"], "created_by": md.created_by, "num_row_groups": md.num_row_groups})
    json.dump({"library": "polars", "version": pl.__version__, "results": results}, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
