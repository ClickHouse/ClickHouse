"""Create Iceberg tables with Apache Spark and the Iceberg Spark runtime, then read every
snapshot of every table back with Spark.

Usage: python spark_iceberg_writer.py <job.json>

The job names the Iceberg runtime version (`iceberg_version`), the Hadoop catalog warehouse,
and the tables to create (a subset of `TABLES`). For each table the result lists its
snapshots, and `<output_dir>/<table>.<snapshot id>.arrow` holds the rows of that snapshot.
"""

import json
import os
import sys


COLUMNS = """
    id BIGINT, i INT, f FLOAT, d DOUBLE, dec DECIMAL(10, 2), dec38 DECIMAL(38, 5), s STRING, bin BINARY, b BOOLEAN,
    dt DATE, ts TIMESTAMP, ts_ntz TIMESTAMP_NTZ, st STRUCT<x: INT, y: STRING>, arr ARRAY<INT>, mp MAP<STRING, INT>
"""


def rows(lo, hi):
    """`INSERT ... SELECT` producing deterministic rows `lo <= id < hi` for `COLUMNS`."""
    return f"""
    SELECT id, CAST(id * 7 - 100 AS INT), CAST(id / 4 AS FLOAT), id * 1.5e10, CAST(id * 3.25 AS DECIMAL(10, 2)),
        CAST(id * 123456789.12345 AS DECIMAL(38, 5)), IF(id % 5 = 0, NULL, concat('s', CAST(id AS STRING))),
        CAST(concat('b', CAST(id AS STRING)) AS BINARY), id % 2 = 0, date_add(DATE '1970-01-01', CAST(id * 400 - 4000 AS INT)),
        timestamp_micros(id * 86400123456 - 1000000000000), CAST(timestamp_micros(id * 3600000001) AS TIMESTAMP_NTZ),
        IF(id % 7 = 0, NULL, named_struct('x', CAST(id AS INT), 'y', CAST(id % 3 AS STRING))),
        IF(id % 6 = 0, NULL, array(CAST(id AS INT), NULL, CAST(id + 1 AS INT))),
        IF(id % 4 = 0, NULL, map('k', CAST(id AS INT), 'z', CAST(-id AS INT)))
    FROM range({lo}, {hi}) t(id)"""


def simple(lo, hi):
    return f"SELECT id, concat('v', CAST(id AS STRING)) AS s, CAST(id % 4 AS INT) AS p, timestamp_micros(id * 7200000000) AS ts FROM range({lo}, {hi}) t(id)"


MOR = "'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"

# Table name -> list of SQL statements; `{t}` is the table identifier.
TABLES = {
    "v1_all_types": [
        f"CREATE TABLE {{t}} ({COLUMNS}) USING iceberg TBLPROPERTIES ('format-version' = '1')",
        f"INSERT INTO {{t}} {rows(0, 20)}",
        f"INSERT INTO {{t}} {rows(20, 35)}",
    ],
    "v2_all_types": [
        f"CREATE TABLE {{t}} ({COLUMNS}) USING iceberg TBLPROPERTIES ('format-version' = '2')",
        f"INSERT INTO {{t}} {rows(0, 30)}",
    ],
    "v3_all_types": [
        f"CREATE TABLE {{t}} ({COLUMNS}) USING iceberg TBLPROPERTIES ('format-version' = '3')",
        f"INSERT INTO {{t}} {rows(0, 30)}",
    ],
    "v2_identity_days": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (p, days(ts)) TBLPROPERTIES ('format-version' = '2')",
        f"INSERT INTO {{t}} {simple(0, 40)}",
    ],
    "v2_transforms": [
        """CREATE TABLE {t} (id BIGINT, s STRING, n INT, d DATE, ts TIMESTAMP, ts2 TIMESTAMP_NTZ, b BOOLEAN)
           USING iceberg PARTITIONED BY (bucket(4, id), truncate(3, s), truncate(10, n), years(d), hours(ts), months(ts2), b)
           TBLPROPERTIES ('format-version' = '2')""",
        """INSERT INTO {t} SELECT id, concat('val', CAST(id % 7 AS STRING)), CAST(id * 3 AS INT), date_add(DATE '1999-06-01', CAST(id * 100 AS INT)),
           timestamp_micros(id * 5400000000), CAST(timestamp_micros(id * 2592000000000) AS TIMESTAMP_NTZ), id % 3 = 0 FROM range(0, 40) t(id)""",
    ],
    "v2_position_deletes": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version' = '2', {MOR})",
        f"INSERT INTO {{t}} {simple(0, 60)}",
        "DELETE FROM {t} WHERE id % 3 = 0",
        "UPDATE {t} SET s = concat(s, '_upd') WHERE id % 5 = 1",
        "DELETE FROM {t} WHERE p = 2 AND id > 40",
    ],
    "v3_deletion_vectors": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version' = '3', {MOR})",
        f"INSERT INTO {{t}} {simple(0, 60)}",
        "DELETE FROM {t} WHERE id % 3 = 0",
        "DELETE FROM {t} WHERE id % 5 = 0",
        "UPDATE {t} SET s = 'updated' WHERE id = 7",
    ],
    "v2_copy_on_write": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version' = '2')",
        f"INSERT INTO {{t}} {simple(0, 30)}",
        f"INSERT INTO {{t}} {simple(30, 50)}",
        "DELETE FROM {t} WHERE id % 4 = 1",
        "UPDATE {t} SET s = 'cow' WHERE id < 5",
        f"INSERT OVERWRITE {{t}} SELECT * FROM ({simple(100, 110)}) WHERE p = 0",
        "CALL local.system.rewrite_data_files(table => 'db.{n}')",
        "CALL local.system.rewrite_manifests('db.{n}')",
    ],
    "v2_schema_evolution": [
        "CREATE TABLE {t} (a INT, b STRING, c FLOAT, d DECIMAL(5, 2), st STRUCT<x: INT>) USING iceberg TBLPROPERTIES ('format-version' = '2')",
        "INSERT INTO {t} SELECT CAST(id AS INT), concat('b', CAST(id AS STRING)), CAST(id AS FLOAT) / 2, CAST(id AS DECIMAL(5, 2)), named_struct('x', CAST(id AS INT)) FROM range(0, 5) t(id)",
        "ALTER TABLE {t} ADD COLUMN e STRING",
        "ALTER TABLE {t} RENAME COLUMN b TO b_renamed",
        "INSERT INTO {t} SELECT CAST(id AS INT), concat('b', CAST(id AS STRING)), CAST(id AS FLOAT) / 2, CAST(id AS DECIMAL(5, 2)), named_struct('x', CAST(id AS INT)), concat('e', CAST(id AS STRING)) FROM range(5, 10) t(id)",
        "ALTER TABLE {t} ALTER COLUMN a TYPE BIGINT",
        "ALTER TABLE {t} ALTER COLUMN c TYPE DOUBLE",
        "ALTER TABLE {t} ALTER COLUMN d TYPE DECIMAL(9, 2)",
        "ALTER TABLE {t} ADD COLUMN st.y STRING",
        "ALTER TABLE {t} DROP COLUMN e",
        "ALTER TABLE {t} ADD COLUMN e INT",
        "ALTER TABLE {t} ALTER COLUMN e FIRST",
        "INSERT INTO {t} SELECT CAST(id AS INT), id * 10000000000, concat('b', CAST(id AS STRING)), id / 3, CAST(id * 1000 AS DECIMAL(9, 2)), named_struct('x', CAST(id AS INT), 'y', 'yy') FROM range(10, 15) t(id)",
    ],
    "v2_partition_evolution": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (p) TBLPROPERTIES ('format-version' = '2')",
        f"INSERT INTO {{t}} {simple(0, 20)}",
        "ALTER TABLE {t} ADD PARTITION FIELD days(ts)",
        f"INSERT INTO {{t}} {simple(20, 40)}",
        "ALTER TABLE {t} DROP PARTITION FIELD p",
        "ALTER TABLE {t} REPLACE PARTITION FIELD days(ts) WITH bucket(3, id)",
        f"INSERT INTO {{t}} {simple(40, 60)}",
    ],
    "v1_partition_evolution_void": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg PARTITIONED BY (p, truncate(2, s)) TBLPROPERTIES ('format-version' = '1')",
        f"INSERT INTO {{t}} {simple(0, 20)}",
        "ALTER TABLE {t} DROP PARTITION FIELD p",
        f"INSERT INTO {{t}} {simple(20, 40)}",
    ],
    "v2_sorted": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version' = '2')",
        "ALTER TABLE {t} WRITE ORDERED BY p DESC NULLS LAST, id",
        f"INSERT INTO {{t}} {simple(0, 40)}",
    ],
    "v2_gzip_metadata": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.metadata.compression-codec' = 'gzip')",
        f"INSERT INTO {{t}} {simple(0, 25)}",
        f"INSERT INTO {{t}} {simple(25, 30)}",
    ],
    "v2_orc": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'orc')",
        f"INSERT INTO {{t}} {simple(0, 25)}",
    ],
    "v2_avro": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'avro')",
        f"INSERT INTO {{t}} {simple(0, 25)}",
    ],
    "v2_mixed_formats_codecs": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')",
        f"INSERT INTO {{t}} {simple(0, 10)}",
        "ALTER TABLE {t} SET TBLPROPERTIES ('write.parquet.compression-codec' = 'gzip')",
        f"INSERT INTO {{t}} {simple(10, 20)}",
        "ALTER TABLE {t} SET TBLPROPERTIES ('write.parquet.compression-codec' = 'uncompressed', 'write.parquet.page-row-limit' = '3')",
        f"INSERT INTO {{t}} {simple(20, 30)}",
        "ALTER TABLE {t} SET TBLPROPERTIES ('write.format.default' = 'orc', 'write.orc.compression-codec' = 'snappy')",
        f"INSERT INTO {{t}} {simple(30, 40)}",
        "ALTER TABLE {t} SET TBLPROPERTIES ('write.format.default' = 'parquet', 'write.parquet.compression-codec' = 'snappy', 'write.parquet.bloom-filter-enabled.column.id' = 'true')",
        f"INSERT INTO {{t}} {simple(40, 50)}",
    ],
    "v2_position_deletes_unpartitioned": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT, ts TIMESTAMP) USING iceberg TBLPROPERTIES ('format-version' = '2', {MOR})",
        f"INSERT INTO {{t}} {simple(0, 30)}",
        f"INSERT INTO {{t}} {simple(30, 60)}",
        "DELETE FROM {t} WHERE id BETWEEN 10 AND 40",
        "MERGE INTO {t} AS x USING (SELECT id, 'merged' AS s FROM range(50, 70) t(id)) AS y ON x.id = y.id "
        "WHEN MATCHED THEN UPDATE SET x.s = y.s WHEN NOT MATCHED THEN INSERT (id, s, p, ts) VALUES (y.id, y.s, 9, NULL)",
    ],
}

# Tables created with the older Iceberg runtime (Avro manifests by an older writer, older metadata layout).
OLD_TABLES = ["v1_all_types", "v2_all_types", "v2_position_deletes", "v2_schema_evolution", "v2_transforms"]


def main():
    # Imported here: the generator imports this module for `TABLES` without Spark installed.
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.pandas.types import to_arrow_schema

    job = json.load(open(sys.argv[1]))
    version = job["iceberg_version"]
    warehouse = job["warehouse"]
    spark = (
        SparkSession.builder.master("local[1]").appName("data-lake-corpus-iceberg")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{version}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    results = []
    for base in job["tables"]:
        name = job.get("prefix", "") + base
        ident = f"local.db.{name}"
        statements = [st.format(t=ident, n=name) for st in TABLES[base]]
        for stmt in statements:
            spark.sql(stmt)
        snaps = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {ident}.snapshots ORDER BY committed_at").collect()
        snapshots = []
        for s in snaps:
            df = spark.sql(f"SELECT * FROM {ident} VERSION AS OF {s.snapshot_id}")
            batches = df._collect_as_arrow()
            table = pa.Table.from_batches(batches, schema=to_arrow_schema(df.schema)) if batches else to_arrow_schema(df.schema).empty_table()
            with pa.OSFile(os.path.join(job["output_dir"], f"{name}.{s.snapshot_id}.arrow"), "wb") as f:
                w = ipc.new_file(f, table.schema)
                w.write_table(table)
                w.close()
            snapshots.append({"snapshot_id": s.snapshot_id, "timestamp_ms": int(s.committed_at.timestamp() * 1000), "operation": s.operation})
        results.append({"name": name, "base": base, "location": f"{warehouse}/db/{name}", "statements": [" ".join(st.split()) for st in statements], "snapshots": snapshots})
    json.dump({"library": "spark+iceberg", "version": f"spark {pyspark.__version__}, iceberg {version}", "results": results},
              open(job["result"], "w"), indent=1)
    spark.stop()


if __name__ == "__main__":
    main()
