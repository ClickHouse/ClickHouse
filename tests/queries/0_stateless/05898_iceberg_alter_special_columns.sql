-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- An Iceberg table cannot have ALIAS, MATERIALIZED or EPHEMERAL columns, so ALTER must reject them like CREATE does:
-- otherwise the stored definition cannot be attached again, and the server does not start.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05898_iceberg_alter_special_columns/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (id Int64, d Decimal(5, 0), s String) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');
INSERT INTO t VALUES (1, 12345, 'a');

ALTER TABLE t MODIFY COLUMN d Decimal(14, 0) EPHEMERAL; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t MODIFY COLUMN d Decimal(14, 0) MATERIALIZED 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t MODIFY COLUMN d Nullable(Decimal(38, 0)) ALIAS '10737418.24'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t ADD COLUMN a Nullable(Int64) ALIAS id + 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t ADD COLUMN m Nullable(Int64) MATERIALIZED id; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t ADD COLUMN e Nullable(Int64) EPHEMERAL; -- { serverError BAD_ARGUMENTS }

-- The rejected statements did not change the Iceberg schema.
CREATE TABLE t2 ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');
DESCRIBE t2;
DROP TABLE t2;

DETACH TABLE t;
ATTACH TABLE t;
SELECT * FROM t ORDER BY id;

ALTER TABLE t MODIFY COLUMN d Decimal(14, 0);
SELECT id, d, toTypeName(d) FROM t ORDER BY id;

-- `t_stale` keeps `y` after `t` drops it, so its `IF NOT EXISTS` is skipped by ClickHouse but not by the latest Iceberg schema.
ALTER TABLE t ADD COLUMN y Nullable(String);
CREATE TABLE t_stale ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');
ALTER TABLE t DROP COLUMN y;
ALTER TABLE t_stale ADD COLUMN IF NOT EXISTS y Nullable(String) ALIAS 'a'; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_stale;
CREATE TABLE t2 ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');
DESCRIBE t2;
DROP TABLE t2;

DROP TABLE t SYNC;
DROP TABLE iceberg_path;
