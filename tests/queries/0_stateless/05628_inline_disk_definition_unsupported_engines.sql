-- Tags: no-fasttest
-- Tag no-fasttest: `IcebergLocal` requires USE_AVRO

-- Only the `MergeTree` family creates a disk from an inline `disk(...)` definition, other engines with a `disk` setting reject it.

CREATE TABLE t_log (a UInt64) ENGINE = Log SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_tiny_log (a UInt64) ENGINE = TinyLog SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_stripe_log (a UInt64) ENGINE = StripeLog SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_set (a UInt64) ENGINE = Set SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_join (a UInt64) ENGINE = Join(ANY, LEFT, a) SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_iceberg (a UInt64) ENGINE = IcebergLocal('/tmp/inline_disk_definition_iceberg/', 'Parquet') SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM icebergLocal('/tmp/inline_disk_definition_iceberg/', 'Parquet', SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/')); -- { serverError BAD_ARGUMENTS }
