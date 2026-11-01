"""Write Parquet files with DuckDB (`COPY ... TO ... (FORMAT parquet, ...)`), read back with DuckDB.

Usage: python duckdb_writer.py <job.json>

A job either converts an input Arrow table (`input`) or runs its own `sql` producing
DuckDB-specific types (UUID, INTERVAL, HUGEINT, ENUM, TIME, UNION, ...).
"""

import json
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq


def main():
    job = json.load(open(sys.argv[1]))
    results = []
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for j in job["jobs"]:
        out = os.path.join(job["output_dir"], j["name"] + ".parquet")
        if j.get("sql"):
            query = j["sql"]
        else:
            with pa.memory_map(os.path.join(job["input_dir"], j["input"])) as f:
                src = ipc.open_file(f).read_all()  # noqa: F841 (referenced by name from SQL)
            query = "SELECT * FROM src"
        opts = ", ".join(["FORMAT parquet"] + [f"{k} {v}" for k, v in j.get("options", {}).items()])
        con.execute(f"COPY ({query}) TO '{out}' ({opts})")
        res = con.execute(f"SELECT * FROM read_parquet('{out}')")
        # `fetch_arrow_table` was renamed to `to_arrow_table` in DuckDB 2.0.
        back = res.to_arrow_table() if hasattr(res, "to_arrow_table") else res.fetch_arrow_table()
        with pa.OSFile(os.path.join(job["output_dir"], j["name"] + ".arrow"), "wb") as f:
            w = ipc.new_file(f, back.schema)
            w.write_table(back)
            w.close()
        md = pq.ParquetFile(out).metadata
        results.append({"name": j["name"], "created_by": md.created_by, "num_row_groups": md.num_row_groups})
    json.dump({"library": "duckdb", "version": duckdb.__version__, "results": results}, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
