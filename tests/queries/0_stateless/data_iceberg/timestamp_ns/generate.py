# Generates the Iceberg v3 tables in this directory: timestamp_ns / timestamptz_ns columns written through
# the Java Iceberg API, because Spark and PyIceberg cannot write nanosecond timestamps. One data file per
# row, so min/max bounds are tight. Run with pyspark 3.5 and Java 17:
#     python generate.py /tmp/iceberg_ns && (cd /tmp/iceberg_ns && find . -name '*.crc' -delete) && cp -R /tmp/iceberg_ns/* .
import shutil
import sys

from pyspark.sql import SparkSession

out = sys.argv[1]
shutil.rmtree(out, ignore_errors=True)

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
    .getOrCreate()
)
jvm = spark._jvm
ice = jvm.org.apache.iceberg
Types = ice.types.Types

fields = jvm.java.util.ArrayList()
fields.add(Types.NestedField.required(1, "id", Types.IntegerType.get()))
fields.add(Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withoutZone()))
fields.add(Types.NestedField.optional(3, "tstz", Types.TimestampNanoType.withZone()))
schema = ice.Schema(fields)

NANOS = [
    1704067200123456789,  # 2024-01-01 00:00:00.123456789
    1704067200123456788,  # one nanosecond earlier
    1704153599999999999,  # last nanosecond of 2024-01-01
    1704153600000000000,  # 2024-01-02 00:00:00
    -1,  # 1969-12-31 23:59:59.999999999
    -1500,
    -2208988800000000001,  # 1899-12-31 23:59:59.999999999
    -9223372036854775807,  # 1677-09-21, near the int64 limit
    9223372036854775807,  # 2262-04-11 23:47:16.854775807
    None,
]


def build_spec(kind):
    builder = ice.PartitionSpec.builderFor(schema)
    if kind == "bucket":
        builder = builder.bucket("ts", 16)
    elif kind != "unpartitioned":
        builder = getattr(builder, kind)("ts")
    return builder.build()


# Partition values come from Java Iceberg's own PartitionKey, which takes `ts` as a Java Long. Spark builds
# it, because py4j turns small numbers into Integer, and InternalRecordWrapper in Iceberg 1.10 does not
# convert nanosecond timestamps.
def partition_source(nanos):
    value = "NULL" if nanos is None else nanos
    row = spark.sql(f"SELECT 0 AS id, CAST({value} AS BIGINT) AS ts, CAST({value} AS BIGINT) AS tstz")._jdf.first()
    names = jvm.java.util.ArrayList()
    for name in ["id", "ts", "tstz"]:
        names.add(name)
    values = jvm.scala.collection.JavaConverters.mapAsJavaMap(row.getValuesMap(jvm.scala.collection.JavaConverters.asScalaBuffer(names).toSeq()))
    return ice.data.GenericRecord.create(long_ts).copy(values)


# The table schema with the timestamps as longs, so field positions match the table.
long_ts_fields = jvm.java.util.ArrayList()
long_ts_fields.add(Types.NestedField.required(1, "id", Types.IntegerType.get()))
long_ts_fields.add(Types.NestedField.optional(2, "ts", Types.LongType.get()))
long_ts_fields.add(Types.NestedField.optional(3, "tstz", Types.LongType.get()))
long_ts = Types.StructType.of(long_ts_fields)

props = jvm.java.util.HashMap()
props.put("format-version", "3")
tables = ice.hadoop.HadoopTables(spark._jsc.hadoopConfiguration())
for kind in ["unpartitioned", "identity", "day", "hour", "bucket"]:
    spec = build_spec(kind)
    table = tables.create(schema, spec, props, f"{out}/{kind}")
    factory = ice.data.GenericAppenderFactory(table.schema(), table.spec())
    key = ice.PartitionKey(table.spec(), table.schema())
    append = table.newAppend()
    for i, nanos in enumerate(NANOS):
        record = ice.data.GenericRecord.create(table.schema())
        record.setField("id", i + 1)
        if nanos is not None:
            record.setField("ts", ice.util.DateTimeUtil.timestampFromNanos(nanos))
            record.setField("tstz", ice.util.DateTimeUtil.timestamptzFromNanos(nanos))
        key.partition(partition_source(nanos))
        output = table.io().newOutputFile(f"{out}/{kind}/data/{i + 1:02d}.parquet")
        writer = factory.newDataWriter(
            ice.encryption.EncryptedFiles.plainAsEncryptedOutput(output),
            ice.FileFormat.PARQUET,
            key.copy() if spec.isPartitioned() else None,
        )
        writer.write(record)
        writer.close()
        append.appendFile(writer.toDataFile())
    append.commit()
    print(kind, table.currentSnapshot().snapshotId())
spark.stop()
