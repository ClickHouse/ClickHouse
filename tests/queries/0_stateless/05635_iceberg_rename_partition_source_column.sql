-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- `RENAME COLUMN` of the source column of a partition field of an Iceberg table failed with `UNKNOWN_IDENTIFIER`:
-- the `PARTITION BY` clause of the stored table definition was not updated, while Iceberg refers to the column by its id.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05635_iceberg_rename_partition_source_column/') AS path,
    replaceOne(path, '/05635_iceberg_rename_partition_source_column/', '/05635_iceberg_rename_partition_source_column_nested_sort/') AS nested_sort_path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (id Int64, ts DateTime64(6)) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet') PARTITION BY icebergHour(ts);
INSERT INTO t VALUES (1, '2026-01-01 10:00:00'), (2, '2026-01-01 11:00:00');

ALTER TABLE t RENAME COLUMN ts TO event_time;
INSERT INTO t VALUES (3, '2026-01-01 12:00:00');

SELECT id, event_time FROM t ORDER BY id;
SELECT id FROM t WHERE event_time >= '2026-01-01 11:00:00' ORDER BY id;

-- The stored definition is valid.
DETACH TABLE t;
ATTACH TABLE t;
SELECT id, event_time FROM t ORDER BY id;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't' AND create_table_query LIKE '%PARTITION BY icebergHour(event_time)%';

-- A sort key on a subcolumn of a renamed tuple column.
CREATE TABLE ts (id Int64, s Tuple(x Int64)) ENGINE = IcebergLocal((SELECT nested_sort_path FROM iceberg_path), 'Parquet') ORDER BY s.x;
INSERT INTO ts VALUES (1, tuple(20)), (2, tuple(10));
SELECT id, s.x FROM ts ORDER BY id; -- the first read loads the sort order of the table into its in-memory metadata
ALTER TABLE ts RENAME COLUMN s TO renamed_s;
-- The whole tuple is read: its element in the files written before the rename is https://github.com/ClickHouse/ClickHouse/issues/128304.
INSERT INTO ts VALUES (3, tuple(30));
SELECT id, renamed_s FROM ts ORDER BY id;
DETACH TABLE ts;
ATTACH TABLE ts;
SELECT id, renamed_s FROM ts ORDER BY id;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'ts' AND create_table_query LIKE '%ORDER BY %renamed_s.x%';
