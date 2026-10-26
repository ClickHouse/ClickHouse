-- `apply_string_filters_during_scan` reads the values that do not match a substring condition of PREWHERE
-- as empty strings. The input-bytes estimate is based on the in-memory size of the read columns, so it
-- has to account for the replaced values, which were read from disk all the same. Otherwise it is many
-- times too small, and the decision to enable parallel replicas is taken on a fraction of the real input.
-- A compact part has no per-column sizes, so the reader counts the compressed blocks it reads instead.

SET use_uncompressed_cache=0;

-- The columns cache serves repeated reads from memory, which lowers `ReadCompressedBytes` below the
-- estimate for every query after the first one.
SET use_columns_cache=0;

-- The query condition cache would skip the granules without matches on the second run.
SET use_query_condition_cache=0;

-- The injection reads some ranges twice, so one query reads more than another one with the same data, and the estimates
-- of the reads served from a cache are compared with what another query reads.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0;

SET apply_string_filters_during_scan=1, optimize_move_to_prewhere=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=4, max_block_size=8192;

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_compact_unread_compressible;
DROP TABLE IF EXISTS t_compact_unread_incompressible;
DROP TABLE IF EXISTS t_compact_compressible_first_values;
DROP TABLE IF EXISTS t_compact_compressible_first_values_inline_sizes;
DROP TABLE IF EXISTS t_compact_repetitive_long_values_zstd;
DROP TABLE IF EXISTS t_compact_small_blocks;
DROP TABLE IF EXISTS t_compact_cached_column;
DROP TABLE IF EXISTS t_compact_cached_short_rows;

-- Statistics are disabled so that reading them is not accounted in `ReadCompressedBytes`.
-- A wide part has per-column sizes, a compact part does not, and the estimate is taken differently for the two.
CREATE TABLE t_wide(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

-- Each column compressed apart: with blocks shared by the columns, a granule served from the columns cache for one column
-- is loaded from the part all the same for another one, and the queries of the columns cache below read from disk.
CREATE TABLE t_compact(id UInt64, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18, compress_per_column_in_compact_parts=1;

-- Only the column that is read counts: here the column that is not read compresses either much better or much worse
-- than the one that is. The columns are compressed separately, otherwise reading one of them reads the other one as well.
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

-- The first values replaced compress to almost nothing, and the many values after them do not compress at all.
-- Both with the sizes in a stream of their own and inline.
CREATE TABLE t_compact_compressible_first_values(id UInt64, s String CODEC(LZ4)) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18,
    serialization_info_version='with_types', string_serialization_version='with_size_stream', ratio_of_defaults_for_sparse_serialization=1;
INSERT INTO t_compact_compressible_first_values SELECT number, multiIf(number = 0, concat('needle', repeat('x', 74)), number < 256, repeat('ab', 40),
    arrayStringConcat(arrayMap(i -> reinterpretAsString(cityHash64(number, i)), range(10)))) FROM numbers(8192);

CREATE TABLE t_compact_compressible_first_values_inline_sizes(id UInt64, s String CODEC(LZ4)) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18,
    serialization_info_version='with_types', string_serialization_version='single_stream', ratio_of_defaults_for_sparse_serialization=1;
INSERT INTO t_compact_compressible_first_values_inline_sizes SELECT * FROM t_compact_compressible_first_values;

-- The values are 1 MiB each and repetitive, so that they compress to very few bytes.
CREATE TABLE t_compact_repetitive_long_values_zstd(id UInt64, s String CODEC(ZSTD(3))) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, index_granularity_bytes=1073741824, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18;
INSERT INTO t_compact_repetitive_long_values_zstd SELECT number, concat(if(number = 0, 'needle', ''), repeat(hex(cityHash64(number)), 65536)) FROM numbers(64);

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
SELECT count() FROM t_compact_unread_compressible WHERE s LIKE '%needle%' SETTINGS log_comment='query_compact_unread_compressible';
SELECT count() FROM t_compact_unread_incompressible WHERE s LIKE '%needle%' SETTINGS log_comment='query_compact_unread_incompressible';
-- Each condition is a step of PREWHERE of its own, read by a reader of its own.
SELECT count() FROM t_compact_unread_incompressible WHERE s LIKE '%needle%' AND payload LIKE '%A%'
SETTINGS enable_multiple_prewhere_read_steps=1, log_comment='query_compact_two_steps';
SELECT count() FROM t_compact_compressible_first_values WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='query_compact_compressible_first_values';
SELECT count() FROM t_compact_compressible_first_values_inline_sizes WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='query_compact_compressible_first_values_inline_sizes';
SELECT count() FROM t_compact_repetitive_long_values_zstd WHERE s LIKE '%needle%' SETTINGS max_threads=1, log_comment='query_compact_repetitive_long_values_zstd';
SELECT count() FROM t_compact_small_blocks WHERE s LIKE '%needle%' SETTINGS max_threads=1, max_block_size=1, log_comment='query_compact_block_of_one_row';

-- The blocks taken decompressed from the uncompressed cache are read all the same: the second query reads
-- nothing from disk, and its estimate is the same as the one of the first query.
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS use_uncompressed_cache=1, log_comment='cache_1';
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS use_uncompressed_cache=1, log_comment='cache_2';

-- The rows served from the columns cache are not read from the part, so the estimate is taken from a sample of
-- the columns then, and the second query reads nothing from disk.
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS use_columns_cache=1, enable_reads_from_columns_cache=1, enable_writes_to_columns_cache=1,
    columns_cache_max_bytes_to_write_to_cache=0, columns_cache_max_estimated_bytes_to_write_to_cache=0, log_comment='columns_cache_1';
SELECT count() FROM t_compact WHERE s LIKE '%needle%' SETTINGS use_columns_cache=1, enable_reads_from_columns_cache=1, enable_writes_to_columns_cache=1,
    columns_cache_max_bytes_to_write_to_cache=0, columns_cache_max_estimated_bytes_to_write_to_cache=0, log_comment='columns_cache_2';

-- One column of the read is served from the columns cache, the other one is read from the part and filtered: the cache is
-- not written to, otherwise the filter does not apply. The estimate is compared with the same query reading both columns
-- from the part. The condition on `a` needs its values, not only their sizes.
CREATE TABLE t_compact_cached_column(id UInt64, a String, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18, compress_per_column_in_compact_parts=1;
INSERT INTO t_compact_cached_column SELECT id, concat(hex(cityHash64(id, 5)), 'abcdefgh'), s FROM t_wide WHERE id < 100000;
SELECT max(a) FROM t_compact_cached_column SETTINGS use_columns_cache=1, enable_reads_from_columns_cache=1, enable_writes_to_columns_cache=1,
    columns_cache_max_bytes_to_write_to_cache=0, columns_cache_max_estimated_bytes_to_write_to_cache=0, enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;
SELECT count() FROM t_compact_cached_column WHERE s LIKE '%needle%' AND cityHash64(a) != 0
SETTINGS enable_multiple_prewhere_read_steps=0, use_columns_cache=1, enable_reads_from_columns_cache=1, enable_writes_to_columns_cache=0, log_comment='cached_column_1';
SELECT count() FROM t_compact_cached_column WHERE s LIKE '%needle%' AND cityHash64(a) != 0
SETTINGS enable_multiple_prewhere_read_steps=0, use_columns_cache=0, log_comment='cached_column_0';

-- Only the first granule of the entry of a stripe of the cache is read, and its values are much shorter than those of the
-- other granules of the stripe. A granule served from the cache is counted as the part has it.
CREATE TABLE t_compact_cached_short_rows(id UInt64, a String, s String) ENGINE=MergeTree ORDER BY id
SETTINGS auto_statistics_types='', index_granularity=8192, index_granularity_bytes='10Mi', min_bytes_for_wide_part=1e18, min_rows_for_wide_part=1e18,
    compress_per_column_in_compact_parts=1;
INSERT INTO t_compact_cached_short_rows SELECT number, if(number < 8192, hex(number), repeat(hex(cityHash64(number)), 16)),
    concat(if(number % 100 = 0, 'needle', ''), hex(cityHash64(number, 1)), hex(cityHash64(number, 2))) FROM numbers(65536);
SELECT max(a) FROM t_compact_cached_short_rows SETTINGS use_columns_cache=1, enable_reads_from_columns_cache=1, enable_writes_to_columns_cache=1,
    columns_cache_max_bytes_to_write_to_cache=0, columns_cache_max_estimated_bytes_to_write_to_cache=0, enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;
SELECT count() FROM t_compact_cached_short_rows WHERE id < 8192 AND s LIKE '%needle%' AND cityHash64(a) != 0
SETTINGS enable_multiple_prewhere_read_steps=0, use_columns_cache=1, enable_reads_from_columns_cache=1, enable_writes_to_columns_cache=0, log_comment='cached_short_rows_1';
SELECT count() FROM t_compact_cached_short_rows WHERE id < 8192 AND s LIKE '%needle%' AND cityHash64(a) != 0
SETTINGS enable_multiple_prewhere_read_steps=0, use_columns_cache=0, log_comment='cached_short_rows_0';

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

SELECT
    sumIf(ProfileEvents['ReadCompressedBytes'], log_comment = 'cache_2') < sumIf(ProfileEvents['ReadCompressedBytes'], log_comment = 'cache_1') / 10,
    sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'cache_2') = sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'cache_1'),
    sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'cache_1') > 0
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'cache_%') AND (type = 'QueryFinish');

SELECT
    read_cached < read / 10,
    hits > 0,
    greatest(estimate, read) / least(estimate, read) < 1.3
FROM (
    SELECT
        sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], log_comment = 'columns_cache_2') AS estimate,
        sumIf(ProfileEvents['ReadCompressedBytes'], log_comment = 'columns_cache_1') AS read,
        sumIf(ProfileEvents['ReadCompressedBytes'], log_comment = 'columns_cache_2') AS read_cached,
        sumIf(ProfileEvents['ColumnsCacheHits'], log_comment = 'columns_cache_2') AS hits
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'columns_cache_%') AND (type = 'QueryFinish')
);

SELECT name, filter_applied, hits > 0, greatest(estimate, read) / least(estimate, read) < 1.3
FROM (
    SELECT
        replaceRegexpOne(log_comment, '_[01]$', '') AS name,
        sumIf(ProfileEvents['RuntimeDataflowStatisticsInputBytes'], endsWith(log_comment, '_1')) AS estimate,
        sumIf(ProfileEvents['ReadCompressedBytes'], endsWith(log_comment, '_0')) AS read,
        sumIf(ProfileEvents['ColumnsCacheHits'], endsWith(log_comment, '_1')) AS hits,
        sumIf(ProfileEvents['StringValueFilterValuesReplaced'], endsWith(log_comment, '_1')) > 0 AS filter_applied
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'cached_%') AND (type = 'QueryFinish')
    GROUP BY name
)
ORDER BY name;

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_compact_unread_compressible;
DROP TABLE t_compact_unread_incompressible;
DROP TABLE t_compact_compressible_first_values;
DROP TABLE t_compact_compressible_first_values_inline_sizes;
DROP TABLE t_compact_repetitive_long_values_zstd;
DROP TABLE t_compact_small_blocks;
DROP TABLE t_compact_cached_column;
DROP TABLE t_compact_cached_short_rows;
