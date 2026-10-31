"""Read Parquet files written by ClickHouse with a third-party library (the reverse direction of
the corpus), writing each readback as Arrow IPC or recording the error.

Usage: python reader.py <job.json>, where the job is {"library", "files", "output_dir", "result"}.
"""

import json
import os
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def read(library, path):
    if library == "pyarrow":
        import pyarrow.parquet as pq
        return pq.read_table(path)
    if library == "duckdb":
        import duckdb
        res = duckdb.connect().execute(f"SELECT * FROM read_parquet('{path}')")
        return res.to_arrow_table() if hasattr(res, "to_arrow_table") else res.fetch_arrow_table()
    if library == "polars":
        import polars as pl
        return pl.read_parquet(path).to_arrow()
    if library == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql.pandas.types import to_arrow_schema
        spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate()
        df = spark.read.parquet(path)
        batches = df._collect_as_arrow()
        return pa.Table.from_batches(batches, schema=to_arrow_schema(df.schema)) if batches else to_arrow_schema(df.schema).empty_table()
    if library == "fastparquet":
        import fastparquet
        return pa.Table.from_pandas(fastparquet.ParquetFile(path).to_pandas(), preserve_index=False)
    raise ValueError(library)


def main():
    job = json.load(open(sys.argv[1]))
    results = {}
    for path in job["files"]:
        name = os.path.basename(path)
        try:
            table = read(job["library"], path)
        except BaseException as e:  # the outcome for this file is the error (polars panics are not `Exception`), recorded in the result
            results[name] = {"error": f"{type(e).__name__}: {e}"[:500]}
            continue
        with pa.OSFile(os.path.join(job["output_dir"], name + ".arrow"), "wb") as f:
            w = ipc.new_file(f, table.schema)
            w.write_table(table)
            w.close()
        results[name] = {"rows": table.num_rows}
    json.dump(results, open(job["result"], "w"), indent=1)


if __name__ == "__main__":
    main()
