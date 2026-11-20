-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- A sorted INSERT into an Iceberg table must not mistake a column named like a sort key expression for the key: https://github.com/ClickHouse/ClickHouse/issues/133178

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05838_iceberg_sort_key_named_like_column/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

-- The stored column was replaced by the computed key.
CREATE TABLE t (x String, `icebergTruncate(1, x)` String) ENGINE = IcebergLocal((SELECT path || 't/' FROM iceberg_path), 'Parquet') ORDER BY icebergTruncate(1, x);
INSERT INTO t VALUES ('b', 'a'), ('a', 'b');
SELECT x, `icebergTruncate(1, x)` FROM t ORDER BY x;
SELECT * FROM file(currentDatabase() || '/05838_iceberg_sort_key_named_like_column/t/data/*.parquet', Parquet) SETTINGS max_threads = 1;

-- A key of another type than the column of the same name failed every INSERT with AMBIGUOUS_COLUMN_NAME.
CREATE TABLE b (x String, `icebergBucket(16, x)` String) ENGINE = IcebergLocal((SELECT path || 'b/' FROM iceberg_path), 'Parquet') ORDER BY icebergBucket(16, x);
INSERT INTO b VALUES ('c', 'p'), ('a', 'q'), ('e', 'r'), ('r', 's');
SELECT x, `icebergBucket(16, x)` FROM b ORDER BY x;
SELECT * FROM file(currentDatabase() || '/05838_iceberg_sort_key_named_like_column/b/data/*.parquet', Parquet) SETTINGS max_threads = 1;
SELECT groupArray(icebergBucket(16, x)) FROM (SELECT x FROM file(currentDatabase() || '/05838_iceberg_sort_key_named_like_column/b/data/*.parquet', Parquet)) SETTINGS max_threads = 1;

-- The identity of the stored column made the transform key resolve to the stored column, so the file was sorted by it.
CREATE TABLE c (x String, `icebergTruncate(1, x)` String) ENGINE = IcebergLocal((SELECT path || 'c/' FROM iceberg_path), 'Parquet') ORDER BY (icebergTruncate(1, x), `icebergTruncate(1, x)`);
INSERT INTO c VALUES ('b', 'a'), ('a', 'b');
SELECT x, `icebergTruncate(1, x)` FROM c ORDER BY x;
SELECT * FROM file(currentDatabase() || '/05838_iceberg_sort_key_named_like_column/c/data/*.parquet', Parquet) SETTINGS max_threads = 1;

-- The partition source named like the key was read from the computed key.
CREATE TABLE p (x String, `icebergTruncate(1, x)` String) ENGINE = IcebergLocal((SELECT path || 'p/' FROM iceberg_path), 'Parquet') PARTITION BY `icebergTruncate(1, x)` ORDER BY icebergTruncate(1, x);
INSERT INTO p VALUES ('bb', 'k'), ('ab', 'k'), ('cc', 'm');
SELECT x, `icebergTruncate(1, x)` FROM p ORDER BY x;
SELECT arraySort(groupArray(x)) AS xs FROM p GROUP BY _path ORDER BY xs;

-- A data file merged from several sorted blocks was merged by the column instead of the key.
CREATE TABLE s (x String, `icebergTruncate(1, x)` String) ENGINE = IcebergLocal((SELECT path || 's/' FROM iceberg_path), 'Parquet') ORDER BY icebergTruncate(1, x);
INSERT INTO s SELECT char(97 + number % 26) || toString(number), char(122 - number % 26) FROM numbers(2400)
SETTINGS max_block_size = 1000, max_insert_block_size = 1000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 1, max_insert_threads = 1;
SELECT count(), countIf(`icebergTruncate(1, x)` = char(219 - ascii(x))) FROM s;
SELECT count(), countIf(sorted) FROM (SELECT groupArray(substring(x, 1, 1)) AS keys, keys = arraySort(keys) AS sorted FROM s GROUP BY _file) SETTINGS max_threads = 1, input_format_parquet_preserve_order = 1;
