-- Inserts into a `Merge` table are forwarded into its table to write by a nested `INSERT` query.

SELECT '--- the columns that the table to write does not have are not written';
CREATE TABLE t_target (a UInt64, b String DEFAULT 'default') ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_merge (a UInt8, c String) ENGINE = Merge(currentDatabase(), '^t_target$', t_target);
INSERT INTO t_merge VALUES (1, 'not written');
SELECT * FROM t_target ORDER BY ALL;

SELECT '--- no columns in common';
CREATE TABLE t_merge_no_common (x UInt8) ENGINE = Merge(currentDatabase(), '^t_target$', t_target);
INSERT INTO t_merge_no_common VALUES (1); -- { serverError INCOMPATIBLE_COLUMNS }

SELECT '--- INSERT SELECT with several insert threads';
INSERT INTO t_merge (a) SELECT number % 200 FROM numbers(1000) SETTINGS max_insert_threads = 4, max_block_size = 100, min_insert_block_size_rows = 100;
SELECT count(), sum(a), uniqExact(b) FROM t_target;

SELECT '--- Alias of a Merge table';
CREATE TABLE t_alias ENGINE = Alias(currentDatabase(), 't_merge');
INSERT INTO t_alias VALUES (250, 'not written');
SELECT * FROM t_target WHERE a = 250;

SELECT '--- the materialized view of the table to write sees the converted data';
CREATE TABLE t_mv_dst (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW t_mv TO t_mv_dst AS SELECT a * 10 AS a, b FROM t_target;
INSERT INTO t_merge VALUES (255, 'not written');
SELECT * FROM t_mv_dst ORDER BY ALL;
