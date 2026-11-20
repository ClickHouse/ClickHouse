-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- Reading an Iceberg table in order must not take a column named like a sort key expression for the key.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
-- Three files in two groups: the in-order read merges each group before the final merge.
SET optimize_read_in_order = 1, max_threads = 4, read_in_order_two_level_merge_threshold = 2;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05839_iceberg_read_in_order_key_named_like_column/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (x String, y String) ENGINE = IcebergLocal((SELECT path || 't/' FROM iceberg_path), 'Parquet') ORDER BY icebergTruncate(1, x);
INSERT INTO t VALUES ('b', 'a');
INSERT INTO t VALUES ('a', 'b');
INSERT INTO t VALUES ('c', 'q');

-- The column renamed to the name of the key was read as the key.
ALTER TABLE t RENAME COLUMN y TO `icebergTruncate(1, x)`;
SELECT x, `icebergTruncate(1, x)` FROM t ORDER BY icebergTruncate(1, x);

-- A key of another type than the column failed the read with AMBIGUOUS_COLUMN_NAME.
CREATE TABLE b (x String, y String) ENGINE = IcebergLocal((SELECT path || 'b/' FROM iceberg_path), 'Parquet') ORDER BY icebergBucket(16, x);
INSERT INTO b VALUES ('c', 'p');
INSERT INTO b VALUES ('a', 'q');
INSERT INTO b VALUES ('e', 'r');
ALTER TABLE b RENAME COLUMN y TO `icebergBucket(16, x)`;
SELECT x, `icebergBucket(16, x)` FROM b ORDER BY icebergBucket(16, x);

-- With the column also in the key, the order by the column was taken from the order by the key, also without the two-level merge.
CREATE TABLE c (x String, y String) ENGINE = IcebergLocal((SELECT path || 'c/' FROM iceberg_path), 'Parquet') ORDER BY (icebergTruncate(1, x), y);
INSERT INTO c VALUES ('a', 'z'), ('b', 'a');
INSERT INTO c VALUES ('c', 'b'), ('d', 'c');
INSERT INTO c VALUES ('e', 'd');
ALTER TABLE c RENAME COLUMN y TO `icebergTruncate(1, x)`;
SELECT x, `icebergTruncate(1, x)` FROM c ORDER BY `icebergTruncate(1, x)` SETTINGS max_threads = 1, read_in_order_two_level_merge_threshold = 100;
