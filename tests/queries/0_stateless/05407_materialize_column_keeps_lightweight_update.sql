-- MATERIALIZE COLUMN of a column the part does not store must keep the values set by a lightweight UPDATE.
-- https://github.com/ClickHouse/ClickHouse/issues/126611

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_mat_lwu_compact;
DROP TABLE IF EXISTS t_mat_lwu_wide;
DROP TABLE IF EXISTS t_mat_lwu_input;

SELECT 'compact';
CREATE TABLE t_mat_lwu_compact (id Int64, x Int64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = '1G';
INSERT INTO t_mat_lwu_compact SELECT number, number FROM numbers(4);
ALTER TABLE t_mat_lwu_compact ADD COLUMN a Int64 DEFAULT 7;
UPDATE t_mat_lwu_compact SET a = -1, x = -100 WHERE id IN (1, 2);
ALTER TABLE t_mat_lwu_compact MATERIALIZE COLUMN a;
SELECT id, x, a FROM t_mat_lwu_compact ORDER BY id SETTINGS apply_patch_parts = 0;

SELECT 'wide';
CREATE TABLE t_mat_lwu_wide (id Int64, x Int64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_mat_lwu_wide SELECT number, number FROM numbers(4);
ALTER TABLE t_mat_lwu_wide ADD COLUMN a Int64 DEFAULT id * 10;
UPDATE t_mat_lwu_wide SET a = -1 WHERE id IN (1, 2);
ALTER TABLE t_mat_lwu_wide MATERIALIZE COLUMN a;
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_mat_lwu_wide' AND active AND name NOT LIKE 'patch-%';
SELECT id, a FROM t_mat_lwu_wide ORDER BY id SETTINGS apply_patch_parts = 0;

-- A patch of a DEFAULT input only: the column is still computed from the patched input.
SELECT 'patched input';
CREATE TABLE t_mat_lwu_input (id Int64, x Int64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_mat_lwu_input SELECT number, number FROM numbers(4);
ALTER TABLE t_mat_lwu_input ADD COLUMN a Int64 DEFAULT x * 10;
UPDATE t_mat_lwu_input SET x = 100 WHERE id = 1;
ALTER TABLE t_mat_lwu_input MATERIALIZE COLUMN a;
SELECT id, x, a FROM t_mat_lwu_input ORDER BY id SETTINGS apply_patch_parts = 0;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mat_lwu_input' AND active AND name NOT LIKE 'patch-%' AND column = 'a';

DROP TABLE t_mat_lwu_compact;
DROP TABLE t_mat_lwu_wide;
DROP TABLE t_mat_lwu_input;
