-- ALTER TABLE ... REWRITE PARTS chooses the serialization kind of each column again from the counts stored in the part, as merges do.

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_no_counts;
DROP TABLE IF EXISTS t_batch;
DROP TABLE IF EXISTS t_patch;
DROP TABLE IF EXISTS t_index;

CREATE TABLE t_wide (id UInt64, s String, u UInt64, t Tuple(a UInt64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

CREATE TABLE t_compact (id UInt64, s String, u UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

CREATE TABLE t_no_counts (id UInt64, s String, u UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, compute_exact_num_defaults_for_sparse_columns = 1;

CREATE TABLE t_batch (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

CREATE TABLE t_patch (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1,
    enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

CREATE TABLE t_index (id UInt64, s String, INDEX idx_s s TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

-- s: 80% defaults, u: 99% defaults, t.a: 80% defaults, t.b: 99% defaults.
INSERT INTO t_wide SELECT number, if(number % 5 = 0, 'x', ''), if(number % 100 = 0, number, 0), (if(number % 5 = 0, number, 0), if(number % 100 = 0, 'y', ''))
FROM numbers(10000)
SETTINGS max_block_size = 65505, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

INSERT INTO t_compact SELECT number, if(number % 5 = 0, 'x', ''), if(number % 100 = 0, number, 0)
FROM numbers(10000)
SETTINGS max_block_size = 65505, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

INSERT INTO t_no_counts SELECT number, if(number % 5 = 0, 'x', ''), if(number % 100 = 0, number, 0)
FROM numbers(10000)
SETTINGS max_block_size = 65505, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

INSERT INTO t_batch SELECT number, if(number % 5 = 0, 'x', '')
FROM numbers(10000)
SETTINGS max_block_size = 65505, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

INSERT INTO t_patch SELECT number, if(number % 5 = 0, 'x', '')
FROM numbers(10000)
SETTINGS max_block_size = 65505, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

INSERT INTO t_index SELECT number, if(number % 5 = 0, 'x', '')
FROM numbers(10000)
SETTINGS max_block_size = 65505, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- { echo }

SELECT table, count(), groupUniqArray(part_type) FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY table;

-- Wide part: inserted with ratio 0.9.
SELECT column, serialization_kind, subcolumns.serializations[indexOf(subcolumns.names, 'a')] AS a, subcolumns.serializations[indexOf(subcolumns.names, 'b')] AS b
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide' AND active AND column IN ('s', 'u', 't') ORDER BY column;
SELECT count(), countIf(s != ''), sum(u), sum(t.a), countIf(t.b != '') FROM t_wide;

ALTER TABLE t_wide MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_wide REWRITE PARTS SETTINGS mutations_sync = 2;
SELECT column, serialization_kind, subcolumns.serializations[indexOf(subcolumns.names, 'a')] AS a, subcolumns.serializations[indexOf(subcolumns.names, 'b')] AS b
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide' AND active AND column IN ('s', 'u', 't') ORDER BY column;
SELECT count(), countIf(s != ''), sum(u), sum(t.a), countIf(t.b != '') FROM t_wide;

ALTER TABLE t_wide MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.995;
ALTER TABLE t_wide REWRITE PARTS SETTINGS mutations_sync = 2;
SELECT column, serialization_kind, subcolumns.serializations[indexOf(subcolumns.names, 'a')] AS a, subcolumns.serializations[indexOf(subcolumns.names, 'b')] AS b
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide' AND active AND column IN ('s', 'u', 't') ORDER BY column;
SELECT count(), countIf(s != ''), sum(u), sum(t.a), countIf(t.b != '') FROM t_wide;

CHECK TABLE t_wide SETTINGS check_query_single_value_result = 1;
DETACH TABLE t_wide;
ATTACH TABLE t_wide;
SELECT column, serialization_kind, subcolumns.serializations[indexOf(subcolumns.names, 'a')] AS a, subcolumns.serializations[indexOf(subcolumns.names, 'b')] AS b
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide' AND active AND column IN ('s', 'u', 't') ORDER BY column;
SELECT count(), countIf(s != ''), sum(u), sum(t.a), countIf(t.b != '') FROM t_wide;

-- Compact part.
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_compact' AND active AND column IN ('s', 'u') ORDER BY column;
ALTER TABLE t_compact MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_compact REWRITE PARTS SETTINGS mutations_sync = 2;
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_compact' AND active AND column IN ('s', 'u') ORDER BY column;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
SELECT count(), countIf(s != ''), sum(u) FROM t_compact;

-- A part written with ratio 1 stores no counts: the first rewrite records them, the second one chooses from them.
ALTER TABLE t_no_counts MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_no_counts REWRITE PARTS SETTINGS mutations_sync = 2;
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_no_counts' AND active AND column IN ('s', 'u') ORDER BY column;
ALTER TABLE t_no_counts REWRITE PARTS SETTINGS mutations_sync = 2;
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_no_counts' AND active AND column IN ('s', 'u') ORDER BY column;
SELECT count(), countIf(s != ''), sum(u) FROM t_no_counts;

-- The same mutation changes the data: the counts are stale, so the kinds of the source part are kept.
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_batch' AND active AND column = 's';
ALTER TABLE t_batch MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_batch (UPDATE s = 'z' WHERE 1), (REWRITE PARTS) SETTINGS mutations_sync = 2;
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_batch' AND active AND column = 's';
SELECT count(), countIf(s = 'z') FROM t_batch;

-- A pending lightweight update is applied by the rewrite: same.
UPDATE t_patch SET s = 'z' WHERE 1 SETTINGS enable_lightweight_update = 1;
ALTER TABLE t_patch MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_patch REWRITE PARTS SETTINGS mutations_sync = 2;
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_patch' AND active AND column = 's' AND name NOT LIKE 'patch%';
SELECT count(), countIf(s = 'z') FROM t_patch;

-- A skip index does not stop the choice.
ALTER TABLE t_index MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_index REWRITE PARTS SETTINGS mutations_sync = 2;
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_index' AND active AND column = 's';
SELECT count(), countIf(s != '') FROM t_index;

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_no_counts;
DROP TABLE t_batch;
DROP TABLE t_patch;
DROP TABLE t_index;
