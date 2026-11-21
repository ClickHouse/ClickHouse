-- Two v1 patch parts that both update a column added by ADD COLUMN after the part was written.

DROP TABLE IF EXISTS t_lwu_v1_added_column;
CREATE TABLE t_lwu_v1_added_column (id Int64, s String, n Nullable(Int64)) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';

INSERT INTO t_lwu_v1_added_column (id) SELECT number FROM numbers(10);
ALTER TABLE t_lwu_v1_added_column ADD COLUMN a Int64 DEFAULT 7;
UPDATE t_lwu_v1_added_column SET s = 'p1', a = 1 WHERE id IN (1, 2);
UPDATE t_lwu_v1_added_column SET n = 5, a = 2 WHERE id IN (2, 3);

-- The data part does not store `a`.
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_v1_added_column' AND active AND column = 'a' AND NOT startsWith(partition_id, 'patch');

SELECT id, s, n, a FROM t_lwu_v1_added_column WHERE id < 5 ORDER BY id;
SELECT * FROM t_lwu_v1_added_column ORDER BY id;
SELECT id, s, n, a FROM t_lwu_v1_added_column PREWHERE s = 'p1' ORDER BY id;

OPTIMIZE TABLE t_lwu_v1_added_column FINAL;
SELECT id, s, n, a FROM t_lwu_v1_added_column WHERE id < 5 ORDER BY id SETTINGS apply_patch_parts = 0;

DROP TABLE IF EXISTS t_lwu_v1_added_column_apply;
CREATE TABLE t_lwu_v1_added_column_apply (id Int64, s String, n Nullable(Int64)) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';

INSERT INTO t_lwu_v1_added_column_apply (id) SELECT number FROM numbers(10);
ALTER TABLE t_lwu_v1_added_column_apply ADD COLUMN a Int64 DEFAULT 7;
UPDATE t_lwu_v1_added_column_apply SET s = 'p1', a = 1 WHERE id IN (1, 2);
UPDATE t_lwu_v1_added_column_apply SET n = 5, a = 2 WHERE id IN (2, 3);

ALTER TABLE t_lwu_v1_added_column_apply APPLY PATCHES SETTINGS mutations_sync = 2;
SELECT id, s, n, a FROM t_lwu_v1_added_column_apply WHERE id < 5 ORDER BY id SETTINGS apply_patch_parts = 0;

DROP TABLE t_lwu_v1_added_column;
DROP TABLE t_lwu_v1_added_column_apply;
