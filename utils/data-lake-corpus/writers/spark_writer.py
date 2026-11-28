"""Write Parquet files with Apache Spark (parquet-java), read back with Spark.

Usage: python spark_writer.py <job.json>

The input of every job is a Parquet file written by pyarrow, which Spark reads with
parquet-java and rewrites with the job's Spark SQL configuration (`conf`) and Parquet
writer options (`options`). The readback is collected through Spark's Arrow conversion.
"""

import glob
import json
import os
import shutil
import sys

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pyspark
from pyspark.sql import SparkSession

DEFAULT_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.parquet.writeLegacyFormat": "false",
    "spark.sql.parquet.outputTimestampType": "INT96",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
}


def main():
    job = json.load(open(sys.argv[1]))
    spark = (SparkSession.builder.master("local[1]").appName("data-lake-corpus")
             .config("spark.ui.enabled", "false").config("spark.driver.memory", "2g").getOrCreate())
    results = []
    for j in job["jobs"]:
        for k, v in {**DEFAULT_CONF, **j.get("conf", {})}.items():
            spark.conf.set(k, v)
        df = spark.read.parquet(os.path.join(job["input_dir"], j["input"]))
        tmp_dir = os.path.join(job["output_dir"], j["name"] + ".spark")
        writer = df.coalesce(1).write.mode("overwrite")
        for k, v in j.get("options", {}).items():
            writer = writer.option(k, v)
        writer.parquet(tmp_dir)
        parts = glob.glob(os.path.join(tmp_dir, "part-*.parquet"))
        assert len(parts) == 1, parts
        out = os.path.join(job["output_dir"], j["name"] + ".parquet")
        shutil.move(parts[0], out)
        shutil.rmtree(tmp_dir)
        back_df = spark.read.parquet(out)
        if hasattr(back_df, "toArrow"):
            back = back_df.toArrow()
        else:
            batches = back_df._collect_as_arrow()
            schema = batches[0].schema if batches else None
            if schema is None:
                from pyspark.sql.pandas.types import to_arrow_schema
                schema = to_arrow_schema(back_df.schema)
            back = pa.Table.from_batches(batches, schema=schema)
        with pa.OSFile(os.path.join(job["output_dir"], j["name"] + ".arrow"), "wb") as f:
            w = ipc.new_file(f, back.schema)
            w.write_table(back)
            w.close()
        md = pq.ParquetFile(out).metadata
        results.append({"name": j["name"], "created_by": md.created_by, "num_row_groups": md.num_row_groups})
    parquet_version = spark.sparkContext._jvm.org.apache.parquet.Version.FULL_VERSION
    json.dump({"library": "spark", "version": pyspark.__version__, "parquet_java": parquet_version, "results": results},
              open(job["result"], "w"), indent=1)
    spark.stop()


if __name__ == "__main__":
    main()
