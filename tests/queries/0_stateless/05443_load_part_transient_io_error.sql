-- Tags: no-parallel, no-fasttest
-- A single I/O error (EIO) while reading the metadata of a part at table load must not detach the part as broken:
-- the load is retried and the part keeps its rows.
DROP TABLE IF EXISTS t_load_part_io_error;
CREATE TABLE t_load_part_io_error (id UInt64, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_load_part_io_error SELECT number, toString(number) FROM numbers(300);
DETACH TABLE t_load_part_io_error;
SYSTEM ENABLE FAILPOINT merge_tree_load_part_io_error;
ATTACH TABLE t_load_part_io_error;
-- 0: the once-only failpoint is spent, so the ATTACH above did hit the injected I/O error.
SELECT enabled FROM system.fail_points WHERE name = 'merge_tree_load_part_io_error';
SYSTEM DISABLE FAILPOINT merge_tree_load_part_io_error;
SELECT count() FROM t_load_part_io_error;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_load_part_io_error';
DROP TABLE t_load_part_io_error;
