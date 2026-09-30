# Snapshots without schema IDs

Spark 3.1.3 with Iceberg 0.11.1 writes two format-v1 snapshots, containing
`c = 1` and then `c = 1, 2`. That Iceberg release does not write snapshot
`schema-id` fields.

Iceberg 0.12.1 upgrades the table to format v2 through `ALTER TABLE`. This
writes `v4.metadata.json` with a `schemas` array and retains both snapshots
without schema IDs. A subsequent insert writes `v5.metadata.json`, whose
current snapshot has a schema ID while its two historical snapshots do not.
The stateless test reads both metadata versions and the oldest snapshot.

## Regenerate the fixture

Use JDK 11 and PySpark 3.1.3. Download these runtime jars from Maven Central:

```sh
python -m pip install pyspark==3.1.3
curl -fLO https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark3-runtime/0.11.1/iceberg-spark3-runtime-0.11.1.jar
curl -fLO https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark3-runtime/0.12.1/iceberg-spark3-runtime-0.12.1.jar
```

Run each stage in a separate Spark process so that it loads the intended
Iceberg version. Use a generic Spark user so that the fixture does not record
the local login:

```sh
export SPARK_USER=clickhouse HADOOP_USER_NAME=clickhouse SPARK_LOCAL_IP=127.0.0.1
spark-submit --jars iceberg-spark3-runtime-0.11.1.jar generate.py legacy
spark-submit --jars iceberg-spark3-runtime-0.12.1.jar generate.py upgrade
```

The generator refuses to overwrite an existing table in
`/tmp/clickhouse-iceberg-optional-snapshot-schema-id`. It checks the snapshot
fields and reads the rows back through Spark.

Copy the generated table into this directory, excluding Hadoop checksum files:

```sh
table=/tmp/clickhouse-iceberg-optional-snapshot-schema-id/default/iceberg_optional_snapshot_schema_id
cp -R "$table/data" "$table/metadata" .
find data metadata -name '.*.crc' -delete
```

The JSON, Avro, and Parquet files are unchanged Spark output. ClickHouse maps
their original table-location prefix to the test's MinIO directory when reading.
