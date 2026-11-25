-- Tags: no-fasttest, no-replicated-database
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05876_iceberg_alter_if_exists_skipped/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (x Nullable(Int64)) ENGINE = IcebergLocal((SELECT path FROM iceberg_path));
INSERT INTO t VALUES (1);
ALTER TABLE t ADD COLUMN y Nullable(String);
-- A second table on the same Iceberg table, its schema is not refreshed by ALTERs of `t`.
CREATE TABLE t2 ENGINE = IcebergLocal((SELECT path FROM iceberg_path));

CREATE TEMPORARY TABLE metadata_files AS
SELECT count(DISTINCT _file) AS c FROM file(concat((SELECT path FROM iceberg_path), 'metadata/*.metadata.json'), 'LineAsString');

-- Each command below is skipped by its IF [NOT] EXISTS clause.
ALTER TABLE t ADD COLUMN IF NOT EXISTS y DEFAULT 'a';
ALTER TABLE t ADD COLUMN IF NOT EXISTS y Nullable(String);
ALTER TABLE t DROP COLUMN IF EXISTS z;
ALTER TABLE t MODIFY COLUMN IF EXISTS z Nullable(Int64);
ALTER TABLE t RENAME COLUMN IF EXISTS z TO w;

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position;
SELECT * FROM t ORDER BY x;
SELECT count(DISTINCT _file) - (SELECT c FROM metadata_files)
FROM file(concat((SELECT path FROM iceberg_path), 'metadata/*.metadata.json'), 'LineAsString');

-- IF [NOT] EXISTS is decided on the latest schema, not on the stale one of `t2`.
ALTER TABLE t DROP COLUMN y;
SELECT count(DISTINCT _file) - (SELECT c FROM metadata_files)
FROM file(concat((SELECT path FROM iceberg_path), 'metadata/*.metadata.json'), 'LineAsString');
ALTER TABLE t2 ADD COLUMN IF NOT EXISTS y DEFAULT 'a'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t2 ADD COLUMN IF NOT EXISTS y Nullable(String);
ALTER TABLE t ADD COLUMN z Nullable(Int64);
ALTER TABLE t2 DROP COLUMN IF EXISTS z;

DESCRIBE TABLE t SETTINGS describe_compact_output = 1;
SELECT * FROM t ORDER BY x;

DROP TABLE t2;
DROP TABLE t;
