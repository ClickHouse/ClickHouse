-- `apply_string_filters_during_scan` reads the values that do not match a substring condition of PREWHERE
-- as empty strings. The input-bytes estimate is based on the in-memory size of the read columns, so it
-- has to account for the replaced values, which were read from disk all the same. Otherwise it is many
-- times too small, and the decision to enable parallel replicas is taken on a fraction of the real input.

SET use_uncompressed_cache=0;

-- The columns cache serves repeated reads from memory, which lowers `ReadCompressedBytes` below the
-- estimate for every query after the first one.
SET use_columns_cache=0;

-- The query condition cache would skip the granules without matches on the second run.
SET use_query_condition_cache=0;

SET apply_string_filters_during_scan=1, optimize_move_to_prewhere=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=4, max_block_size=8192;

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_compact_unread_compressible;
DROP TABLE IF EXISTS t_compact_unread_incompressible;

-- Statistics are disabled so that reading them is not accounted in `ReadCompressedBytes`.
-- A wide part has per-column sizes, a compact part does not, and the estimate takes the compression ratio differently for the two.
CREATE TABLE t_wide(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

CREATE TABLE t_compact(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;

-- A compact part has the sizes of all its columns together only, which says nothing about the one column read:
-- here the column that is not read compresses either much better or much worse than the one that is.
-- The columns are compressed separately, otherwise reading one of them reads the other one as well.
CREATE TABLE t_compact_unread_compressible(id UInt64, s String, payload String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18, compress_per_column_in_compact_parts=1;

CREATE TABLE t_compact_unread_incompressible(id UInt64, s String, payload String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18, compress_per_column_in_compact_parts=1;

-- One value in a hundred matches; the others are long enough for the filter to stay enabled.
INSERT INTO t_wide SELECT number, concat(if(number % 100 = 0, 'needle', ''), hex(cityHash64(number)), hex(cityHash64(number, 1)), hex(cityHash64(number, 2)), hex(cityHash64(number, 3)), hex(cityHash64(number, 4)))
FROM numbers(300000);
INSERT INTO t_compact SELECT * FROM t_wide;
INSERT INTO t_compact_unread_compressible SELECT id, s, repeat('x', 1000) FROM t_wide WHERE id < 100000;
INSERT INTO t_compact_unread_incompressible SELECT id, concat(if(id % 100 = 0, 'needle', ''), hex(cityHash64(id)), repeat('abcdefgh', 8)), s FROM t_wide WHERE id < 100000;

SELECT count() FROM t_wide WHERE s LIKE '%needle%' SETTINGS log_comment='query_wide';
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS log_comment='query_compact';
SELECT count(), max(id) FROM t_wide WHERE s LIKE '%needle%' SETTINGS log_comment='query_wide_two_columns';
SELECT count() FROM t_compact_unread_compressible WHERE s LIKE '%needle%' SETTINGS log_comment='query_compact_unread_compressible';
SELECT count() FROM t_compact_unread_incompressible WHERE s LIKE '%needle%' SETTINGS log_comment='query_compact_unread_incompressible';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Check that the filter was applied, and that the estimate is close to what was really read.
SELECT log_comment, ProfileEvents['StringValueFilterValuesReplaced'] > 0
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds;

SELECT format('{} {} {}', log_comment, compressed_bytes, statistics_input_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['ReadCompressedBytes'] compressed_bytes,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] statistics_input_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 1.3;

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_compact_unread_compressible;
DROP TABLE t_compact_unread_incompressible;
