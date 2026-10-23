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
DROP TABLE IF EXISTS t_compact_compressibility_changes;
DROP TABLE IF EXISTS t_compact_varying_lengths;
DROP TABLE IF EXISTS t_compact_empty_granules;
DROP TABLE IF EXISTS t_compact_long_values;
DROP TABLE IF EXISTS t_compact_large_matching_value;
DROP TABLE IF EXISTS t_compact_large_replaced_value;
DROP TABLE IF EXISTS t_compact_large_values;
DROP TABLE IF EXISTS t_compact_large_matching_values;
DROP TABLE IF EXISTS t_compact_skip_index;

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

-- In the next two cases the estimate with the filter is compared with the one without it, which samples the same
-- blocks in a single thread: how close either is to what is read depends on the blocks that happen to be sampled.

-- The replaced values are sampled in each sampled block: here they compress well in the first block only.
CREATE TABLE t_compact_compressibility_changes(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_compressibility_changes SELECT number, concat(if(number % 100 = 0, 'needle', ''), if(number < 8192, repeat('abcdefgh', 8),
    concat(reinterpretAsString(cityHash64(number)), reinterpretAsString(cityHash64(number, 1)), reinterpretAsString(cityHash64(number, 2)), reinterpretAsString(cityHash64(number, 3)),
           reinterpretAsString(cityHash64(number, 4)), reinterpretAsString(cityHash64(number, 5)), reinterpretAsString(cityHash64(number, 6)), reinterpretAsString(cityHash64(number, 7)))))
FROM numbers(100000);
SELECT count() FROM t_compact_compressibility_changes WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='same_compressibility_changes_1';
SELECT count() FROM t_compact_compressibility_changes WHERE s LIKE '%needle%' SETTINGS max_threads=1, apply_string_filters_during_scan=0, log_comment='same_compressibility_changes_0';

-- The sizes of the replaced values are read too: here they vary, while the characters compress to nothing.
CREATE TABLE t_compact_varying_lengths(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_varying_lengths SELECT number, concat(if(number % 100 = 0, 'needle', ''), repeat('x', 1 + cityHash64(number) % 64)) FROM numbers(100000);
SELECT count() FROM t_compact_varying_lengths WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='same_varying_lengths_1';
SELECT count() FROM t_compact_varying_lengths WHERE s LIKE '%needle%' SETTINGS max_threads=1, apply_string_filters_during_scan=0, log_comment='same_varying_lengths_0';

-- The sample is taken anew for each block also when only empty values were replaced: here the values of the first
-- five granules either match or are empty, and the sixth granule, which is the next to be sampled, has the usual ones.
-- With the sizes inline, every value is checked by itself, and the filter turns itself off on so many empty values;
-- and the filter does not apply to the `Sparse` serialization, which so many empty values would get.
CREATE TABLE t_compact_empty_granules(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18,
    serialization_info_version='with_types', string_serialization_version='with_size_stream', ratio_of_defaults_for_sparse_serialization=1;
INSERT INTO t_compact_empty_granules SELECT id, if(id < 5 * 8192, if(id % 10 < 7, '', concat('needle', s)), s) FROM t_wide WHERE id < 6 * 8192;
SELECT count() FROM t_compact_empty_granules WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='same_empty_granules_1';
SELECT count() FROM t_compact_empty_granules WHERE s LIKE '%needle%' SETTINGS max_threads=1, apply_string_filters_during_scan=0, log_comment='same_empty_granules_0';

-- A value longer than the sample is sampled along all of it: here a compressible beginning is followed by a long
-- tail that does not compress at all.
CREATE TABLE t_compact_long_values(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_long_values SELECT number, concat(if(number = 0, 'needle', ''), repeat('a', 65536),
    arrayStringConcat(arrayMap(i -> reinterpretAsString(cityHash64(number, i)), range(16384))))
FROM numbers(64);
SELECT count() FROM t_compact_long_values WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='same_long_values_1';
SELECT count() FROM t_compact_long_values WHERE s LIKE '%needle%' SETTINGS max_threads=1, apply_string_filters_during_scan=0, log_comment='same_long_values_0';

-- The matching values and the replaced ones are estimated each by itself: here one long matching value compresses
-- well, and the many short values replaced after it do not compress at all.
CREATE TABLE t_compact_large_matching_value(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_large_matching_value
SELECT 0, concat('needle', repeat('abcd', 262144))
UNION ALL
SELECT number, arrayStringConcat(arrayMap(i -> reinterpretAsString(cityHash64(number, i)), range(10))) FROM numbers(1, 8191);
OPTIMIZE TABLE t_compact_large_matching_value FINAL;
SELECT count() FROM t_compact_large_matching_value WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='same_large_matching_value_1';
SELECT count() FROM t_compact_large_matching_value WHERE s LIKE '%needle%' SETTINGS max_threads=1, apply_string_filters_during_scan=0, log_comment='same_large_matching_value_0';

-- A long replaced value is sampled by itself as well: here it is the first value, it compresses well, and the many
-- short values replaced after it do not compress at all.
CREATE TABLE t_compact_large_replaced_value(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_large_replaced_value
SELECT 0, repeat('abcd', 262144)
UNION ALL
SELECT 1, 'needle'
UNION ALL
SELECT number, arrayStringConcat(arrayMap(i -> reinterpretAsString(cityHash64(number, i)), range(10))) FROM numbers(2, 8190);
OPTIMIZE TABLE t_compact_large_replaced_value FINAL;
SELECT count() FROM t_compact_large_replaced_value WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='same_large_replaced_value_1';
SELECT count() FROM t_compact_large_replaced_value WHERE s LIKE '%needle%' SETTINGS max_threads=1, apply_string_filters_during_scan=0, log_comment='same_large_replaced_value_0';

-- Only a bounded sample of the replaced values is kept to estimate how they compress: they can be long, and holding
-- them in full would take far more memory than the read itself. The values are 1 MiB each, a granule has all of them.
CREATE TABLE t_compact_large_values(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, index_granularity_bytes=1073741824, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_large_values SELECT number, concat(if(number = 0, 'needle', ''), repeat(hex(cityHash64(number)), 65536)) FROM numbers(64);
SELECT count() FROM t_compact_large_values WHERE s LIKE '%needle%' SETTINGS max_threads=1, max_memory_usage='32Mi', log_comment='large_values';

-- The same holds for the matching values, which are restored together with the replaced ones: here the granule
-- has 16 matching values of 1 MiB, and 16 short values that do not match. The memory used to collect the statistics
-- is compared with the memory of the same query without them, because the memory of reading depends on the build.
CREATE TABLE t_compact_large_matching_values(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, index_granularity_bytes=1073741824, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_large_matching_values SELECT number, if(number % 2 = 0,
    concat('needle', arrayStringConcat(arrayMap(i -> reinterpretAsString(cityHash64(number, i)), range(131072)))), hex(cityHash64(number)))
FROM numbers(32);
SELECT count() FROM t_compact_large_matching_values WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='memory_large_matching_values_1';
SELECT count() FROM t_compact_large_matching_values WHERE s LIKE '%needle%'
SETTINGS max_threads=1, enable_parallel_replicas=0, automatic_parallel_replicas_mode=0, log_comment='memory_large_matching_values_0';

-- The parts without matches are rejected by the skip index while reading, and a rejected read task must not
-- use up the blocks to sample, or the part with matches, which comes last, has no sample to estimate the input.
-- `max_rows_to_read` is checked before reading, and the skip indexes are then applied before reading as well.
CREATE TABLE t_compact_skip_index(id UInt64, s String, INDEX idx s TYPE ngrambf_v1(3, 32768, 2, 0) GRANULARITY 1) ENGINE=MergeTree
PARTITION BY intDiv(id, 1024) ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_skip_index SELECT id, s FROM t_wide WHERE id < 32768 AND (id % 100 != 0 OR id >= 31744);
SELECT count() FROM t_compact_skip_index WHERE s LIKE '%needle%'
SETTINGS max_threads=1, use_skip_indexes=1, use_skip_indexes_on_data_read=1, max_rows_to_read=0, log_comment='skip_index';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Check that the filter was applied, and that the estimate is close to what was really read.
SELECT log_comment, ProfileEvents['StringValueFilterValuesReplaced'] > 0
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment = 'skip_index') AND (type = 'QueryFinish');

SELECT name, filter_applied, greatest(with_filter, without_filter) / least(with_filter, without_filter) < 1.1
FROM (
    SELECT
        replaceRegexpOne(log_comment, '_[01]$', '') AS name,
        sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], endsWith(log_comment, '_0')) AS without_filter,
        sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], endsWith(log_comment, '_1')) AS with_filter,
        sumIf(ProfileEvents['StringValueFilterValuesReplaced'], endsWith(log_comment, '_1')) > 0 AS filter_applied
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'same_%') AND (type = 'QueryFinish')
    GROUP BY name
)
ORDER BY name;

SELECT
    sumIf(memory_usage, log_comment = 'memory_large_matching_values_1') / sumIf(memory_usage, log_comment = 'memory_large_matching_values_0') < 1.2,
    sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'memory_large_matching_values_1') > 0
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'memory_%') AND (type = 'QueryFinish');

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
DROP TABLE t_compact_compressibility_changes;
DROP TABLE t_compact_varying_lengths;
DROP TABLE t_compact_empty_granules;
DROP TABLE t_compact_long_values;
DROP TABLE t_compact_large_matching_value;
DROP TABLE t_compact_large_replaced_value;
DROP TABLE t_compact_large_values;
DROP TABLE t_compact_large_matching_values;
DROP TABLE t_compact_skip_index;
