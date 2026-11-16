-- A row whose sign is neither 1 nor -1 is kept as is and cancels no other row: in a merge (horizontal and
-- vertical), in SELECT ... FINAL and in the insert merge (optimize_on_insert).
-- https://github.com/ClickHouse/ClickHouse/issues/131609

DROP TABLE IF EXISTS t_horizontal;
DROP TABLE IF EXISTS t_vertical;
DROP TABLE IF EXISTS t_insert;
DROP TABLE IF EXISTS t_block;

CREATE TABLE t_horizontal (id UInt64, sign Int8, version UInt8, payload UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY id
SETTINGS enable_vertical_merge_algorithm = 0;

CREATE TABLE t_vertical (id UInt64, sign Int8, version UInt8, payload UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY id
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_bytes_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 1,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_insert (id UInt64, sign Int8, version UInt8, payload UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY id;

SET optimize_on_insert = 0;

-- Key 1 is the issue (0 against 1), key 2 a valid pair around an invalid row, key 3 an invalid row between
-- two kept rows (a vertical merge must emit it in read order), key 4 is 0 against -1.
INSERT INTO t_horizontal VALUES (1, 0, 1, 10), (2, 1, 1, 20), (2, 5, 1, 21), (3, 1, 1, 30), (3, 0, 1, 31), (3, 1, 1, 32), (4, 0, 1, 40);
INSERT INTO t_horizontal VALUES (1, 1, 1, 11), (2, -1, 1, 22), (4, -1, 1, 41);
INSERT INTO t_vertical VALUES (1, 0, 1, 10), (2, 1, 1, 20), (2, 5, 1, 21), (3, 1, 1, 30), (3, 0, 1, 31), (3, 1, 1, 32), (4, 0, 1, 40);
INSERT INTO t_vertical VALUES (1, 1, 1, 11), (2, -1, 1, 22), (4, -1, 1, 41);

SELECT 'final', * FROM t_horizontal FINAL ORDER BY id, payload;

OPTIMIZE TABLE t_horizontal FINAL;
OPTIMIZE TABLE t_vertical FINAL;

SELECT 'horizontal', * FROM t_horizontal ORDER BY id, payload;
SELECT 'vertical', * FROM t_vertical ORDER BY id, payload;

SYSTEM FLUSH LOGS part_log;
SELECT DISTINCT 'vertical_algorithm', merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_vertical' AND event_type = 'MergeParts';

INSERT INTO t_insert SETTINGS optimize_on_insert = 1 VALUES
    (1, 0, 1, 10), (1, 1, 1, 11), (2, 1, 1, 20), (2, 5, 1, 21), (2, -1, 1, 22),
    (3, 1, 1, 30), (3, 0, 1, 31), (3, 1, 1, 32), (4, 0, 1, 40), (4, -1, 1, 41);

SELECT 'insert', * FROM t_insert ORDER BY id, payload;

-- A run of such rows read one row per chunk is still returned in blocks of max_block_size rows.
CREATE TABLE t_block (id UInt64, sign Int8, version UInt8, payload UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY id;

SYSTEM STOP MERGES t_block;

INSERT INTO t_block SELECT 1, 0, 1, number FROM numbers(10);
INSERT INTO t_block SELECT 1, 0, 1, number FROM numbers(10, 10);

SELECT 'final_block', max(blockSize()), count() FROM t_block FINAL SETTINGS max_block_size = 1;

DROP TABLE t_horizontal;
DROP TABLE t_vertical;
DROP TABLE t_insert;
DROP TABLE t_block;
