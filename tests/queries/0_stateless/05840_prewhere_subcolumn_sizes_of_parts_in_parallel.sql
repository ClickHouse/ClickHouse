-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the replicas would size the parts, and the test counts the jobs and the reads of the initiator.

-- The PREWHERE optimizer sizes the subcolumns of the parts on up to `max_threads` threads, and the subcolumns of one column
-- of a part with one reader, so that a prefix they share is read once. A reattached part has no sizes in memory ("cold").

SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;
SET optimize_move_to_prewhere = 1, optimize_functions_to_subcolumns = 1;
SET use_statistics = 0, use_statistics_for_part_pruning = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET min_bytes_to_use_mmap_io = 0;
-- The analysis of indexes uses a local thread pool as well.
SET max_threads_for_indexes = 1;

DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_map;

-- No merges: they would survive reattaching, unlike `SYSTEM STOP MERGES`.
CREATE TABLE t_json (d Date, id UInt64, j JSON)
ENGINE = MergeTree PARTITION BY d ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, auto_statistics_types = '',
    max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1;

CREATE TABLE t_map (d Date, id UInt64, m Map(String, String))
ENGINE = MergeTree PARTITION BY d ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, auto_statistics_types = '',
    max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    serialization_info_version = 'with_types';

-- One part per partition.
INSERT INTO t_json SELECT toDate('2026-01-01') + number % 20, number,
    concat('{"k1":', toString(number), ',"k2":', toString(number), '}') FROM numbers(2000)
SETTINGS max_block_size = 65536, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO t_map SELECT toDate('2026-01-01') + number % 20, number,
    map('k1', toString(number), 'k2', toString(number)) FROM numbers(2000)
SETTINGS max_block_size = 65536, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table IN ('t_json', 't_map') AND active AND part_type = 'Wide';

-- Every measured query is cold unless said otherwise: the table is reattached, then a query with the subcolumn sizes off loads
-- what planning reads besides them (the primary index, the column sizes).
DETACH TABLE t_json; ATTACH TABLE t_json;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k1::Int64 > 0) SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k1::Int64 > 0) SETTINGS max_threads = 8, log_comment = 'json_k1' FORMAT Null;

DETACH TABLE t_json; ATTACH TABLE t_json;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k2::Int64 > 0) SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k2::Int64 > 0) SETTINGS max_threads = 8, log_comment = 'json_k2' FORMAT Null;

DETACH TABLE t_json; ATTACH TABLE t_json;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k1::Int64 > 0 AND j.k2::Int64 > 0) SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k1::Int64 > 0 AND j.k2::Int64 > 0) SETTINGS max_threads = 8, log_comment = 'json_both' FORMAT Null;
-- Warm: the sizes are in memory.
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k1::Int64 > 0 AND j.k2::Int64 > 0) SETTINGS max_threads = 8, log_comment = 'json_both_warm' FORMAT Null;

DETACH TABLE t_json; ATTACH TABLE t_json;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k1::Int64 > 0 AND j.k2::Int64 > 0) SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE j.k1::Int64 > 0 AND j.k2::Int64 > 0) SETTINGS max_threads = 1, log_comment = 'json_both_sequential' FORMAT Null;

-- The parts left after partition pruning.
DETACH TABLE t_json; ATTACH TABLE t_json;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE d IN ('2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04') AND j.k1::Int64 > 0 AND j.k2::Int64 > 0)
SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_json WHERE d IN ('2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04') AND j.k1::Int64 > 0 AND j.k2::Int64 > 0)
SETTINGS max_threads = 8, log_comment = 'json_both_pruned' FORMAT Null;

DETACH TABLE t_map; ATTACH TABLE t_map;
SELECT * FROM (EXPLAIN SELECT count() FROM t_map WHERE m['k1'] != '') SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_map WHERE m['k1'] != '') SETTINGS max_threads = 8, log_comment = 'map_k1' FORMAT Null;

DETACH TABLE t_map; ATTACH TABLE t_map;
SELECT * FROM (EXPLAIN SELECT count() FROM t_map WHERE m['k2'] != '') SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_map WHERE m['k2'] != '') SETTINGS max_threads = 8, log_comment = 'map_k2' FORMAT Null;

DETACH TABLE t_map; ATTACH TABLE t_map;
SELECT * FROM (EXPLAIN SELECT count() FROM t_map WHERE m['k1'] != '' AND m['k2'] != '') SETTINGS allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0 FORMAT Null;
SELECT * FROM (EXPLAIN SELECT count() FROM t_map WHERE m['k1'] != '' AND m['k2'] != '') SETTINGS max_threads = 8, log_comment = 'map_both' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The cold queries size the parts in the pool, the warm and the sequential ones use no pool.
SELECT log_comment, ProfileEvents['LocalThreadPoolJobs'] > 0 AS used_pool
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN
    ('json_k1', 'json_k2', 'json_both', 'json_both_warm', 'json_both_sequential', 'json_both_pruned', 'map_k1', 'map_k2', 'map_both')
ORDER BY event_time_microseconds;

-- Two subcolumns of one column read the prefix they share once.
WITH ProfileEvents['ReadCompressedBytes'] AS read
SELECT
    sumIf(read, log_comment = 'json_k1') > 0,
    sumIf(read, log_comment = 'json_both') < sumIf(read, log_comment = 'json_k1') + sumIf(read, log_comment = 'json_k2'),
    sumIf(read, log_comment = 'json_both_sequential') = sumIf(read, log_comment = 'json_both')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish';

WITH ProfileEvents['ReadCompressedBytes'] AS read
SELECT
    sumIf(read, log_comment = 'map_k1') > 0,
    sumIf(read, log_comment = 'map_both') < sumIf(read, log_comment = 'map_k1') + sumIf(read, log_comment = 'map_k2')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish';

DROP TABLE t_json;
DROP TABLE t_map;
