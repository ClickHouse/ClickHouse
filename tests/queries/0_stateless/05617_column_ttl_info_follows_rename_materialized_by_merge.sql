-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: uses the server-global failpoint mt_select_parts_to_mutate_no_free_threads

-- A merge materializes a pending RENAME COLUMN, so it has to re-key the column TTL info of the source parts with
-- the same renames. `max_bytes_to_merge_at_max_space_in_pool = 1` leaves the merges to `OPTIMIZE ... FINAL`.

DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_merge_ttl;
DROP TABLE IF EXISTS t_merge_no_info;

-- TTL merges are stopped, so the merge that materializes the ALTER keeps the TTL info.
CREATE TABLE t_merge (id UInt32, d DateTime, a UInt32 TTL d + INTERVAL 1 YEAR, c UInt32 TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP TTL MERGES t_merge;
INSERT INTO t_merge VALUES (1, now() - 100, 42, 100);
INSERT INTO t_merge VALUES (2, now() - 100, 43, 101);
SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
ALTER TABLE t_merge DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
OPTIMIZE TABLE t_merge FINAL;
SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
SELECT 'merge', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge' AND active;
SELECT 'merge ttl of a is due', column_ttl_min <= now() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_merge' AND active AND column = 'a';
SYSTEM START TTL MERGES t_merge;
OPTIMIZE TABLE t_merge FINAL;
SELECT 'merge', id, a FROM t_merge ORDER BY id;

-- The merge that materializes the ALTER applies TTL, and the dropped column's TTL is the due one.
CREATE TABLE t_merge_ttl (id UInt32, d DateTime, a UInt32 TTL d + INTERVAL 1 SECOND, c UInt32 TTL d + INTERVAL 1 YEAR)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP TTL MERGES t_merge_ttl;
INSERT INTO t_merge_ttl VALUES (1, now() - 100, 42, 100);
INSERT INTO t_merge_ttl VALUES (2, now() - 100, 43, 101);
SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
ALTER TABLE t_merge_ttl DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM START TTL MERGES t_merge_ttl;
OPTIMIZE TABLE t_merge_ttl FINAL;
SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
SELECT 'merge ttl', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_ttl' AND active;
SELECT 'merge ttl', id, a FROM t_merge_ttl ORDER BY id;

-- The renamed column is stored without TTL info, and the dropped column is not stored at all.
CREATE TABLE t_merge_no_info (id UInt32, d DateTime, c UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_merge_no_info VALUES (1, now() - 100, 100);
INSERT INTO t_merge_no_info VALUES (2, now() - 100, 101);
ALTER TABLE t_merge_no_info ADD COLUMN a UInt32 TTL d + INTERVAL 1 YEAR;
ALTER TABLE t_merge_no_info MODIFY COLUMN c UInt32 TTL d + INTERVAL 1 SECOND SETTINGS materialize_ttl_after_modify = 0;
SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
ALTER TABLE t_merge_no_info DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
OPTIMIZE TABLE t_merge_no_info FINAL;
SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
SELECT 'no info', id, a FROM t_merge_no_info ORDER BY id;

DROP TABLE t_merge;
DROP TABLE t_merge_ttl;
DROP TABLE t_merge_no_info;
