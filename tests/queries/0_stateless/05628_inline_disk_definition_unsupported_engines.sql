-- Tags: no-fasttest
-- Tag no-fasttest: `IcebergLocal` and `DataLakeCatalog` require USE_AVRO

-- Only the `MergeTree` family table engines and the `Atomic` and `Ordinary` database engines create a disk from an inline `disk(...)` definition, other engines with a `disk` setting reject it.

CREATE TABLE t_log (a UInt64) ENGINE = Log SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_tiny_log (a UInt64) ENGINE = TinyLog SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_stripe_log (a UInt64) ENGINE = StripeLog SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_set (a UInt64) ENGINE = Set SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_join (a UInt64) ENGINE = Join(ANY, LEFT, a) SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_iceberg (a UInt64) ENGINE = IcebergLocal('/tmp/inline_disk_definition_iceberg/', 'Parquet') SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM icebergLocal('/tmp/inline_disk_definition_iceberg/', 'Parquet', SETTINGS disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/')); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = DataLakeCatalog('http://localhost:1/iceberg') SETTINGS catalog_type = 'rest', disk = disk(type = local, path = '/var/lib/clickhouse/disks/inline/'); -- { serverError BAD_ARGUMENTS }
