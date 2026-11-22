-- Tags: no-parallel
-- no-parallel: the check of the reads served from the uncompressed cache needs the entries to stay there, and the cache is
-- shared with the other tests, which evict them. The cache is cleared for the same reason: the entries left by the previous
-- runs evict those of this one.

-- `apply_string_filters_during_scan` reads the values that do not match a substring condition of PREWHERE
-- as empty strings, although they were read from disk all the same. The input bytes are the compressed blocks the
-- readers load, so the replaced values must be counted as well.

SET use_uncompressed_cache=0;

-- A read served from the columns cache records no statistics, and lowers `ReadCompressedBytes` below the
-- estimate for every query after the first one.
SET use_columns_cache=0;

-- The query condition cache would skip the granules without matches on the second run.
SET use_query_condition_cache=0;

-- The injection reads some ranges twice, so one query reads more than another one with the same data, and the estimate
-- of the read served from the uncompressed cache is compared with what another query reads.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0;

SET apply_string_filters_during_scan=1, optimize_move_to_prewhere=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=4, max_block_size=8192;

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_compact_unread_incompressible;
DROP TABLE IF EXISTS t_compact_small_blocks;

-- Statistics are disabled so that reading them is not accounted in `ReadCompressedBytes`.
CREATE TABLE t_wide(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

CREATE TABLE t_compact(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18, compress_per_column_in_compact_parts=1;

-- Only the column that is read counts, here the column that is not read compresses much worse than the one that is.
-- The columns are compressed separately, otherwise reading one of them reads the other one as well.
CREATE TABLE t_compact_unread_incompressible(id UInt64, s String, payload String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18, compress_per_column_in_compact_parts=1;

-- One value in a hundred matches; the others are long enough for the filter to stay enabled.
INSERT INTO t_wide SELECT number, concat(if(number % 100 = 0, 'needle', ''), hex(cityHash64(number)), hex(cityHash64(number, 1)), hex(cityHash64(number, 2)), hex(cityHash64(number, 3)), hex(cityHash64(number, 4)))
FROM numbers(300000);
INSERT INTO t_compact SELECT * FROM t_wide;
INSERT INTO t_compact_unread_incompressible SELECT id, concat(if(id % 100 = 0, 'needle', ''), hex(cityHash64(id)), repeat('abcdefgh', 8)), s FROM t_wide WHERE id < 100000;

-- A compact part is read in whole granules, so a block can have many more rows than requested: here a granule of
-- 8192 rows is read for a block of 1 row. Only the first row matches, and the replaced values do not compress at all.
CREATE TABLE t_compact_small_blocks(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, index_granularity_bytes='10Mi', min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_small_blocks SELECT number, if(number = 0, 'needle', concat(
    reinterpretAsString(cityHash64(number)), reinterpretAsString(cityHash64(number, 1)), reinterpretAsString(cityHash64(number, 2)), reinterpretAsString(cityHash64(number, 3)), reinterpretAsString(cityHash64(number, 4)),
    reinterpretAsString(cityHash64(number, 5)), reinterpretAsString(cityHash64(number, 6)), reinterpretAsString(cityHash64(number, 7)), reinterpretAsString(cityHash64(number, 8)), reinterpretAsString(cityHash64(number, 9))))
FROM numbers(8192);

SELECT count() FROM t_wide WHERE s LIKE '%needle%' SETTINGS log_comment='query_wide';
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS log_comment='query_compact';
SELECT count(), max(id) FROM t_wide WHERE s LIKE '%needle%' SETTINGS log_comment='query_wide_two_columns';
SELECT count() FROM t_compact_unread_incompressible WHERE s LIKE '%needle%' SETTINGS log_comment='query_compact_unread_incompressible';
-- Each condition is a step of PREWHERE of its own, read by a reader of its own.
SELECT count() FROM t_compact_unread_incompressible WHERE s LIKE '%needle%' AND payload LIKE '%A%'
SETTINGS enable_multiple_prewhere_read_steps=1, log_comment='query_compact_two_steps';
SELECT count() FROM t_compact_small_blocks WHERE s LIKE '%needle%' SETTINGS max_threads=1, max_block_size=1, log_comment='query_compact_block_of_one_row';

-- The blocks taken decompressed from the uncompressed cache are read all the same: the second query reads
-- nothing from disk, and its estimate is the same as the one of the first query.
SYSTEM CLEAR UNCOMPRESSED CACHE;
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS use_uncompressed_cache=1, log_comment='cache_1';
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS use_uncompressed_cache=1, log_comment='cache_2';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Check that the filter was applied, and that the estimate is close to what was really read.
SELECT
    log_comment,
    ProfileEvents['StringValueFilterValuesReplaced'] > 0,
    greatest(ProfileEvents['ReadCompressedBytes'], ProfileEvents['RuntimeDataflowStatisticsInputBytes'])
        <= least(ProfileEvents['ReadCompressedBytes'], ProfileEvents['RuntimeDataflowStatisticsInputBytes']) * 1.3
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds;

SELECT
    sumIf(ProfileEvents['ReadCompressedBytes'], log_comment = 'cache_2') = 0,
    sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'cache_2') = sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'cache_1'),
    sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'cache_1') > 0
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'cache_%') AND (type = 'QueryFinish');

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_compact_unread_incompressible;
DROP TABLE t_compact_small_blocks;
