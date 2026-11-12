"""Create Delta Lake tables with Apache Spark and Delta Lake (`delta-spark`), then read every
version of every table back with Spark.

Usage: python spark_delta_writer.py <job.json>

The job names the tables to create (a subset of `TABLES`, possibly with a name prefix) and the
output directory. For each table the result lists its versions (`DESCRIBE HISTORY`), and
`<output_dir>/<table>.<version>.arrow` holds the rows of that version. For tables with the change
data feed, `<output_dir>/<table>.cdf.arrow` holds `table_changes` from version 0 to the last one.
"""

import json
import os
import sys


COLUMNS = """
    id BIGINT, i INT, sh SMALLINT, by TINYINT, f FLOAT, d DOUBLE, dec DECIMAL(10, 2), dec38 DECIMAL(38, 5), s STRING,
    bin BINARY, b BOOLEAN, dt DATE, ts TIMESTAMP, ts_ntz TIMESTAMP_NTZ, st STRUCT<x: INT, y: STRING>, arr ARRAY<INT>,
    mp MAP<STRING, INT>, nested ARRAY<STRUCT<a: INT, m: MAP<STRING, ARRAY<INT>>>>
"""


def rows(lo, hi):
    """`SELECT` producing deterministic rows `lo <= id < hi` for `COLUMNS`."""
    return f"""
    SELECT id, CAST(id * 7 - 100 AS INT), CAST(id * 3 - 50 AS SMALLINT), CAST(id % 256 - 128 AS TINYINT), CAST(id / 4 AS FLOAT),
        id * 1.5e10, CAST(id * 3.25 AS DECIMAL(10, 2)), CAST(id * 123456789.12345 AS DECIMAL(38, 5)),
        IF(id % 5 = 0, NULL, concat('s', CAST(id AS STRING))), CAST(concat('b', CAST(id AS STRING)) AS BINARY), id % 2 = 0,
        date_add(DATE '1970-01-01', CAST(id * 400 - 4000 AS INT)), timestamp_micros(id * 86400123456 - 1000000000000),
        CAST(timestamp_micros(id * 3600000001) AS TIMESTAMP_NTZ),
        IF(id % 7 = 0, NULL, named_struct('x', CAST(id AS INT), 'y', CAST(id % 3 AS STRING))),
        IF(id % 6 = 0, NULL, array(CAST(id AS INT), NULL, CAST(id + 1 AS INT))),
        IF(id % 4 = 0, NULL, map('k', CAST(id AS INT), 'z', CAST(-id AS INT))),
        array(named_struct('a', CAST(id AS INT), 'm', map('x', array(CAST(id AS INT), CAST(id * 2 AS INT)))))
    FROM range({lo}, {hi}) t(id)"""


def simple(lo, hi):
    return f"SELECT id, concat('v', CAST(id AS STRING)) AS s, CAST(id % 4 AS INT) AS p FROM range({lo}, {hi}) t(id)"


DV = "'delta.enableDeletionVectors' = 'true'"
NO_DV = "'delta.enableDeletionVectors' = 'false'"
CDF = "'delta.enableChangeDataFeed' = 'true'"
CM_NAME = "'delta.columnMapping.mode' = 'name'"
CM_ID = "'delta.columnMapping.mode' = 'id'"

# Table name -> definition. `statements` use `{t}` for the table identifier (delta.`<path>`).
# `conf` are Spark session settings for the statements, `cdf` requests the change data feed readback,
# `readback` replaces `SELECT *` for the readback (for types that Arrow cannot hold).
TABLES = {
    "all_types": {"statements": [
        f"CREATE TABLE {{t}} ({COLUMNS}) USING delta TBLPROPERTIES ({NO_DV})",
        f"INSERT INTO {{t}} {rows(0, 20)}",
        f"INSERT INTO {{t}} {rows(20, 35)}",
    ]},
    "partitioned": {"statements": [
        """CREATE TABLE {t} (id BIGINT, s STRING, p_int INT, p_str STRING, p_date DATE, p_ts_ntz TIMESTAMP_NTZ, p_bool BOOLEAN,
           p_dec DECIMAL(9, 3)) USING delta PARTITIONED BY (p_int, p_str, p_date, p_ts_ntz, p_bool, p_dec)""",
        """INSERT INTO {t} SELECT id, concat('v', CAST(id AS STRING)), IF(id % 5 = 0, NULL, CAST(id % 3 AS INT)),
           CASE id % 4 WHEN 0 THEN NULL WHEN 1 THEN 'a b' WHEN 2 THEN 'x/y=z' ELSE '' END,
           IF(id % 6 = 0, NULL, date_add(DATE '2000-01-01', CAST(id % 2 AS INT))),
           IF(id % 7 = 0, NULL, CAST(timestamp_micros(1600000000000000 + (id % 2) * 1000001) AS TIMESTAMP_NTZ)),
           IF(id % 8 = 0, NULL, id % 2 = 0), IF(id % 9 = 0, NULL, CAST(id / 8 AS DECIMAL(9, 3))) FROM range(0, 30) t(id)""",
    ]},
    "deletion_vectors": {"statements": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta TBLPROPERTIES ({DV}, 'delta.checkpointInterval' = '3')",
        f"INSERT INTO {{t}} {simple(0, 100)}",
        "DELETE FROM {t} WHERE id % 10 = 0",
        "DELETE FROM {t} WHERE id BETWEEN 50 AND 59",
        f"INSERT INTO {{t}} {simple(100, 150)}",
        "DELETE FROM {t} WHERE id % 7 = 0",
        "UPDATE {t} SET s = 'updated' WHERE id % 11 = 1",
    ]},
    "deletion_vectors_partitioned_column_mapping": {"statements": [
        f"""CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta PARTITIONED BY (p)
            TBLPROPERTIES ({DV}, {CM_NAME})""",
        f"INSERT INTO {{t}} {simple(0, 40)}",
        "DELETE FROM {t} WHERE id % 3 = 0",
        "ALTER TABLE {t} RENAME COLUMN s TO s_renamed",
        "DELETE FROM {t} WHERE p = 1 AND id > 20",
        f"INSERT INTO {{t}} {simple(40, 50)}",
    ]},
    "merge": {"statements": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta TBLPROPERTIES ({DV})",
        f"INSERT INTO {{t}} {simple(0, 30)}",
        f"""MERGE INTO {{t}} AS x USING ({simple(20, 40)}) AS y ON x.id = y.id
            WHEN MATCHED AND y.id % 2 = 0 THEN UPDATE SET x.s = 'merged'
            WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED THEN INSERT *""",
    ]},
    "column_mapping_id": {"statements": [
        f"""CREATE TABLE {{t}} (id BIGINT, s STRING, st STRUCT<a: INT, b: STRING>, arr ARRAY<STRUCT<c: INT>>, mp MAP<STRING, STRUCT<e: INT>>)
            USING delta TBLPROPERTIES ({CM_ID}, {NO_DV})""",
        """INSERT INTO {t} SELECT id, concat('v', CAST(id AS STRING)), named_struct('a', CAST(id AS INT), 'b', 'b'),
           array(named_struct('c', CAST(id AS INT))), map('k', named_struct('e', CAST(id AS INT))) FROM range(0, 5) t(id)""",
        "ALTER TABLE {t} RENAME COLUMN s TO s2",
        "ALTER TABLE {t} RENAME COLUMN st.a TO a2",
        "ALTER TABLE {t} DROP COLUMN st.b",
        "ALTER TABLE {t} ADD COLUMN s TINYINT",
        """INSERT INTO {t} SELECT id, concat('w', CAST(id AS STRING)), named_struct('a2', CAST(id AS INT)),
           array(named_struct('c', CAST(id AS INT))), map('k', named_struct('e', CAST(id AS INT))), CAST(id AS TINYINT) FROM range(5, 8) t(id)""",
    ]},
    "schema_evolution": {"statements": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING) USING delta TBLPROPERTIES ({NO_DV})",
        "INSERT INTO {t} SELECT id, concat('v', CAST(id AS STRING)) FROM range(0, 5) t(id)",
        "ALTER TABLE {t} ADD COLUMNS (n INT, st STRUCT<x: INT>)",
        "INSERT INTO {t} SELECT id, concat('v', CAST(id AS STRING)), CAST(id AS INT), named_struct('x', CAST(id AS INT)) FROM range(5, 10) t(id)",
        "ALTER TABLE {t} ADD COLUMNS (st.y STRING AFTER x)",
        "ALTER TABLE {t} ALTER COLUMN n FIRST",
        "INSERT INTO {t} SELECT CAST(id AS INT), id, NULL, named_struct('x', CAST(id AS INT), 'y', 'yy') FROM range(10, 12) t(id)",
        "INSERT OVERWRITE {t} SELECT CAST(id AS INT), id, 'overwritten', named_struct('x', 1, 'y', 'z') FROM range(20, 23) t(id)",
    ]},
    "change_data_feed_deletion_vectors": {"cdf": True, "statements": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta TBLPROPERTIES ({CDF}, {DV})",
        f"INSERT INTO {{t}} {simple(0, 10)}",
        "UPDATE {t} SET s = 'updated' WHERE id IN (1, 2)",
        "DELETE FROM {t} WHERE id >= 8",
        f"INSERT INTO {{t}} {simple(10, 12)}",
    ]},
    "change_data_feed_column_mapping": {"cdf": True, "statements": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta PARTITIONED BY (p) TBLPROPERTIES ({CDF}, {CM_NAME}, {NO_DV})",
        f"INSERT INTO {{t}} {simple(0, 8)}",
        "UPDATE {t} SET s = 'updated' WHERE id = 1",
        "DELETE FROM {t} WHERE id = 4",
    ]},
    "type_widening": {"statements": [
        """CREATE TABLE {t} (id INT, by TINYINT, sh SMALLINT, i INT, f FLOAT, dec DECIMAL(5, 2), dt DATE)
           USING delta TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.feature.timestampNtz' = 'supported')""",
        """INSERT INTO {t} VALUES (1, 1Y, 1S, 1, 1.5, 1.25, DATE'2020-01-01'),
           (2, -128Y, -32768S, -2147483648, -0.5, -999.99, DATE'1970-01-01'), (3, NULL, NULL, NULL, NULL, NULL, NULL)""",
        "ALTER TABLE {t} ALTER COLUMN by TYPE SMALLINT",
        "ALTER TABLE {t} ALTER COLUMN sh TYPE INT",
        "ALTER TABLE {t} ALTER COLUMN i TYPE BIGINT",
        "ALTER TABLE {t} ALTER COLUMN f TYPE DOUBLE",
        "ALTER TABLE {t} ALTER COLUMN dec TYPE DECIMAL(12, 4)",
        "ALTER TABLE {t} ALTER COLUMN dt TYPE TIMESTAMP_NTZ",
        "INSERT INTO {t} VALUES (4, 1000S, 100000, 10000000000, 1.0E300, 12345678.1234, TIMESTAMP_NTZ'2021-06-15 12:34:56.789')",
    ]},
    "type_widening_nested": {"statements": [
        """CREATE TABLE {t} (id INT, st STRUCT<a: SMALLINT, b: FLOAT>, arr ARRAY<INT>, mp MAP<INT, DECIMAL(4, 1)>, p SMALLINT)
           USING delta PARTITIONED BY (p) TBLPROPERTIES ('delta.enableTypeWidening' = 'true')""",
        "INSERT INTO {t} VALUES (1, named_struct('a', 1S, 'b', 0.5), array(1, 2), map(1, 1.5), 1S)",
        "ALTER TABLE {t} ALTER COLUMN st.a TYPE INT",
        "ALTER TABLE {t} ALTER COLUMN st.b TYPE DOUBLE",
        "ALTER TABLE {t} ALTER COLUMN arr.element TYPE BIGINT",
        "ALTER TABLE {t} ALTER COLUMN mp.key TYPE BIGINT",
        "ALTER TABLE {t} ALTER COLUMN mp.value TYPE DECIMAL(10, 3)",
        "ALTER TABLE {t} ALTER COLUMN p TYPE INT",
        "INSERT INTO {t} VALUES (2, named_struct('a', 100000, 'b', 1.0E200), array(10000000000), map(10000000000, 1234567.125), 100000)",
    ]},
    "v2_checkpoint_json": {"conf": {"spark.databricks.delta.checkpointV2.topLevelFileFormat": "json"}, "statements": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT) USING delta TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '3')",
        *[f"INSERT INTO {{t}} {simple(i * 10, i * 10 + 10)}" for i in range(5)],
        "DELETE FROM {t} WHERE id = 5",
    ]},
    "v2_checkpoint_parquet": {"conf": {"spark.databricks.delta.checkpointV2.topLevelFileFormat": "parquet"}, "statements": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT) USING delta TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '3')",
        *[f"INSERT INTO {{t}} {simple(i * 10, i * 10 + 10)}" for i in range(5)],
        "DELETE FROM {t} WHERE id = 5",
    ]},
    "multi_part_checkpoint": {"conf": {"spark.databricks.delta.checkpoint.partSize": "2"}, "statements": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta TBLPROPERTIES ({NO_DV}, 'delta.checkpointInterval' = '4')",
        *[f"INSERT INTO {{t}} {simple(i * 10, i * 10 + 10)}" for i in range(5)],
        "DELETE FROM {t} WHERE id < 15",
    ]},
    "row_tracking_in_commit_timestamps": {"statements": [
        f"""CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta
            TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableInCommitTimestamps' = 'true', {NO_DV})""",
        f"INSERT INTO {{t}} {simple(0, 10)}",
        f"INSERT INTO {{t}} {simple(10, 20)}",
        "UPDATE {t} SET s = 'updated' WHERE id = 3",
        "OPTIMIZE {t}",
    ]},
    "liquid_clustering": {"statements": [
        "CREATE TABLE {t} (id BIGINT, s STRING, p INT) USING delta CLUSTER BY (p, id)",
        *[f"INSERT INTO {{t}} {simple(i * 10, i * 10 + 10)}" for i in range(3)],
        "OPTIMIZE {t}",
        "ALTER TABLE {t} CLUSTER BY (s)",
        f"INSERT INTO {{t}} {simple(30, 35)}",
    ]},
    "iceberg_compat_v2": {"statements": [
        f"""CREATE TABLE {{t}} (id BIGINT, s STRING, mp MAP<STRING, INT>, arr ARRAY<STRUCT<x: INT>>) USING delta
            TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true', {CM_NAME})""",
        "INSERT INTO {t} VALUES (1, 'a', map('x', 1), array(named_struct('x', 1))), (2, NULL, map(), array())",
        "INSERT INTO {t} VALUES (3, 'c', NULL, NULL)",
    ]},
    "writer_features": {"statements": [
        """CREATE TABLE {t} (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 5), s STRING,
           s_len INT GENERATED ALWAYS AS (length(s)), d STRING DEFAULT 'dflt') USING delta
           TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported', 'delta.appendOnly' = 'false')""",
        "ALTER TABLE {t} ADD CONSTRAINT s_not_empty CHECK (s <> '')",
        "INSERT INTO {t} (s) VALUES ('a'), ('bb'), ('ccc')",
        "INSERT INTO {t} (s, d) VALUES ('dddd', 'given')",
    ]},
    "restore": {"statements": [
        f"CREATE TABLE {{t}} (id BIGINT, s STRING, p INT) USING delta TBLPROPERTIES ({DV})",
        f"INSERT INTO {{t}} {simple(0, 10)}",
        "DELETE FROM {t} WHERE id < 5",
        f"INSERT INTO {{t}} {simple(10, 15)}",
        "RESTORE TABLE {t} TO VERSION AS OF 1",
        f"INSERT INTO {{t}} {simple(20, 22)}",
    ]},
    "variant": {"readback": "SELECT id, to_json(v) AS v, s FROM {t}", "statements": [
        "CREATE TABLE {t} (id INT, v VARIANT, s STRING) USING delta",
        """INSERT INTO {t} SELECT * FROM VALUES (1, parse_json('{{"a": 1, "b": [1, 2]}}'), 'x'), (2, parse_json('"str"'), 'y'),
           (3, CAST(NULL AS VARIANT), 'z')""",
    ]},
}

# Tables created with the older Delta Lake (protocol versions and log layout of the older writer).
OLD_TABLES = ["all_types", "partitioned", "deletion_vectors", "column_mapping_id", "schema_evolution",
              "change_data_feed_deletion_vectors", "v2_checkpoint_parquet", "multi_part_checkpoint"]


def main():
    # Imported here: the generator imports this module for `TABLES` without Spark installed.
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyspark
    from delta import configure_spark_with_delta_pip
    from importlib.metadata import version as package_version
    from pyspark.sql import SparkSession
    from pyspark.sql.pandas.types import to_arrow_schema

    job = json.load(open(sys.argv[1]))
    builder = (
        SparkSession.builder.master("local[1]").appName("data-lake-corpus-delta")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def save(df, path):
        batches = df._collect_as_arrow()
        table = pa.Table.from_batches(batches, schema=to_arrow_schema(df.schema)) if batches else to_arrow_schema(df.schema).empty_table()
        with pa.OSFile(path, "wb") as f:
            w = ipc.new_file(f, table.schema)
            w.write_table(table)
            w.close()

    results = []
    for base in job["tables"]:
        definition = TABLES[base]
        name = job.get("prefix", "") + base
        location = os.path.join(job["warehouse"], name)
        ident = f"delta.`{location}`"
        statements = [st.format(t=ident) for st in definition["statements"]]
        for k, v in definition.get("conf", {}).items():
            spark.conf.set(k, v)
        for stmt in statements:
            spark.sql(stmt)
        for k in definition.get("conf", {}):
            spark.conf.unset(k)
        history = spark.sql(f"DESCRIBE HISTORY {ident}").orderBy("version").collect()
        versions = []
        readback = definition.get("readback", "SELECT * FROM {t}")
        for h in history:
            df = spark.sql(readback.format(t=f"{ident} VERSION AS OF {h.version}"))
            save(df, os.path.join(job["output_dir"], f"{name}.{h.version}.arrow"))
            versions.append({"version": h.version, "operation": h.operation})
        result = {"name": name, "base": base, "location": location, "statements": [" ".join(st.split()) for st in statements],
                  "versions": versions}
        if definition.get("cdf"):
            df = spark.sql(f"SELECT * FROM table_changes('{ident}', 0)").drop("_commit_timestamp")
            save(df, os.path.join(job["output_dir"], f"{name}.cdf.arrow"))
            result["cdf"] = True
        protocol = spark.sql(f"DESCRIBE DETAIL {ident}").collect()[0]
        result["protocol"] = {"min_reader_version": protocol.minReaderVersion, "min_writer_version": protocol.minWriterVersion,
                              "table_features": sorted(protocol.tableFeatures or [])}
        results.append(result)
    json.dump({"library": "spark+delta", "version": f"spark {pyspark.__version__}, delta {package_version('delta-spark')}",
               "results": results}, open(job["result"], "w"), indent=1)
    spark.stop()


if __name__ == "__main__":
    main()
