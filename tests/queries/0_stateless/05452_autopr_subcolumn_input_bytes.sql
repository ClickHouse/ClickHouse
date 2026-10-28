-- The analyzer turns `length(s)` and `s <> ''` into reads of the subcolumn `s.size`. The input bytes that
-- automatic parallel replicas records for such a read must be priced by what is read from disk for `s.size`.
-- They used to inherit the compression ratio of the other columns read next to it: here the key compresses
-- several hundred times better than the sizes, so the recorded input was far below the bytes on disk.

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_wide_single_stream;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_renamed;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_nested_compact;
DROP TABLE IF EXISTS t_codec;
DROP TABLE IF EXISTS t_codec_renamed;

-- `s.size` has a stream of its own.
CREATE TABLE t_wide (key UInt64, s String) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'with_size_stream',
    serialization_info_version = 'with_types', ratio_of_defaults_for_sparse_serialization = 1,
    index_granularity = 8192, index_granularity_bytes = '10Mi';

-- The sizes are kept inside the only stream of `s`, so reading `s.size` reads all of `s`.
CREATE TABLE t_wide_single_stream (key UInt64, s String) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'single_stream',
    ratio_of_defaults_for_sparse_serialization = 1;

-- With `compress_per_column_in_compact_parts`, each column of each granule is a compressed block of its own: the granules
-- have to be as large as in `t_wide` to compare with its sizes.
CREATE TABLE t_compact (key UInt64, s String) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, compress_per_column_in_compact_parts = 1, string_serialization_version = 'with_size_stream',
    serialization_info_version = 'with_types', ratio_of_defaults_for_sparse_serialization = 1,
    index_granularity = 8192, index_granularity_bytes = '10Mi';

-- Renamed while the mutation that renames the files in the part is held back: the part still names the column `s`.
CREATE TABLE t_renamed AS t_wide;

-- `length(n.a)` reads the offsets `n.size0` that `n.a` shares with `n.b`.
CREATE TABLE t_nested (key UInt64, n Nested(a String, b UInt64)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1, ratio_of_defaults_for_sparse_serialization = 1;

-- A compact part reads the offsets as `n.a.size0`, not as the shared `n.size0`.
CREATE TABLE t_nested_compact (key UInt64, n Nested(a String, b UInt64)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, compress_per_column_in_compact_parts = 1, share_nested_offsets = 1, ratio_of_defaults_for_sparse_serialization = 1;

-- A compact part is sampled with the `CODEC` of the column, which must be found under its old name too.
CREATE TABLE t_codec (key UInt64, s String CODEC(NONE)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, compress_per_column_in_compact_parts = 1, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_codec_renamed AS t_codec;

INSERT INTO t_wide SELECT intDiv(number, 20000), repeat('x', cityHash64(number) % 50) FROM numbers(2e5);
INSERT INTO t_wide_single_stream SELECT * FROM t_wide;
INSERT INTO t_compact SELECT * FROM t_wide;
INSERT INTO t_renamed SELECT * FROM t_wide;
INSERT INTO t_nested SELECT intDiv(number, 20000), arrayMap(i -> 'x', range(cityHash64(number) % 5)), arrayMap(i -> i, range(cityHash64(number) % 5))
FROM numbers(2e5);

INSERT INTO t_nested_compact SELECT * FROM t_nested;
INSERT INTO t_codec SELECT * FROM t_wide;
INSERT INTO t_codec_renamed SELECT * FROM t_wide;

SYSTEM STOP MERGES t_renamed;
ALTER TABLE t_renamed RENAME COLUMN s TO renamed SETTINGS mutations_sync = 0, alter_sync = 0;
SYSTEM STOP MERGES t_codec_renamed;
ALTER TABLE t_codec_renamed RENAME COLUMN s TO renamed SETTINGS mutations_sync = 0, alter_sync = 0;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_functions_to_subcolumns = 1;

SELECT key, avg(length(s)) FROM t_wide WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_wide';
SELECT key, avg(length(s)) FROM t_wide_single_stream WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_wide_single_stream';
-- `s` and `s.size` read the one stream of `s`.
SELECT key, max(s), max(s.size) FROM t_wide_single_stream GROUP BY key FORMAT Null SETTINGS log_comment = '05452_wide_single_stream_full_and_size';
SELECT key, avg(length(s)) FROM t_compact WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_compact';
SELECT key, avg(length(renamed)) FROM t_renamed WHERE renamed <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_renamed';
SELECT key, avg(length(n.a)) FROM t_nested GROUP BY key FORMAT Null SETTINGS log_comment = '05452_nested';
-- Both read the one stream `n.size0`.
SELECT key, avg(length(n.a)), avg(length(n.b)) FROM t_nested GROUP BY key FORMAT Null SETTINGS log_comment = '05452_nested_shared_stream';
-- `n.b` read whole shares `n.size0` with `n.a.size0`, although the part counts it in the size of `n.a` only.
SELECT key, avg(length(n.a)), sum(arraySum(n.b)) FROM t_nested GROUP BY key FORMAT Null SETTINGS log_comment = '05452_nested_offsets_and_full';
SELECT key, avg(length(n.a)) FROM t_nested_compact GROUP BY key FORMAT Null SETTINGS log_comment = '05452_nested_compact';
SELECT key, max(s) FROM t_codec GROUP BY key FORMAT Null SETTINGS log_comment = '05452_codec';
SELECT key, max(renamed) FROM t_codec_renamed GROUP BY key FORMAT Null SETTINGS log_comment = '05452_codec_renamed';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- Compact parts compress each column apart (`compress_per_column_in_compact_parts`): with blocks shared by the
-- columns, reading one of them loads the others too.
-- The bytes on disk of what each query reads: `key`, plus the stream of `s.size` if it has one and all of `s`
-- otherwise, or the offsets `n.size0` that `n.a` and `n.b` share. The compact tables report no per-column sizes;
-- they hold the same rows as `t_wide` and `t_nested`, and so does the renamed table, whose part is still the one
-- written for `t_wide`'s columns. A stream read by several columns is charged once, and the per-row pricing of
-- wide parts is exact, so those cases get a tight bound; the others are sampled or priced by a ratio.
WITH on_disk AS
(
    SELECT table, sum(multiIf(
        column = 'key', column_data_compressed_bytes,
        column = 's' AND has(subcolumns.names, 'size'), subcolumns.data_compressed_bytes[indexOf(subcolumns.names, 'size')],
        column = 's', column_data_compressed_bytes,
        column = 'n.a', subcolumns.data_compressed_bytes[indexOf(subcolumns.names, 'size0')],
        0)) AS bytes
    FROM system.parts_columns
    WHERE database = currentDatabase() AND active AND table IN ('t_wide', 't_wide_single_stream', 't_nested')
    GROUP BY table
)
SELECT log_comment, input_bytes_match_disk FROM
(
SELECT
    log_comment,
    if(log_comment IN ('05452_nested', '05452_nested_shared_stream', '05452_wide_single_stream_full_and_size'), 1.25, 2) AS tolerance,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] BETWEEN on_disk.bytes / tolerance AND on_disk.bytes * tolerance AS input_bytes_match_disk
FROM system.query_log
JOIN on_disk ON on_disk.table = multiIf(
    startsWith(log_comment, '05452_wide_single_stream'), 't_wide_single_stream',
    startsWith(log_comment, '05452_nested'), 't_nested',
    't_wide')
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment IN ('05452_wide', '05452_wide_single_stream', '05452_compact', '05452_renamed', '05452_nested',
        '05452_nested_shared_stream', '05452_wide_single_stream_full_and_size')
    AND type = 'QueryFinish'
)
ORDER BY log_comment;

-- A compact part reports no per-column sizes, and keeps the offsets of each array of a `Nested` apart: what it reads
-- for `n.a.size0` is at least the shared offsets of the wide part, and at most the whole part.
WITH
    (SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] FROM system.query_log
     WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
        AND log_comment = '05452_nested_compact' AND type = 'QueryFinish') AS compact,
    (SELECT sum(subcolumns.data_compressed_bytes[indexOf(subcolumns.names, 'size0')]) FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_nested' AND active AND column = 'n.a') AS offsets,
    (SELECT sum(data_compressed_bytes) FROM system.parts
     WHERE database = currentDatabase() AND table = 't_nested_compact' AND active) AS part_bytes
SELECT compact BETWEEN offsets * 0.8 AND part_bytes * 1.25 AS nested_compact_offsets_priced;

-- What is read next to the offsets is the data of `n.b`, which is at most the size the part gives `n.b`.
WITH
    (SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] FROM system.query_log
     WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
        AND log_comment = '05452_nested_offsets_and_full' AND type = 'QueryFinish') AS offsets_and_full,
    (SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] FROM system.query_log
     WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
        AND log_comment = '05452_nested' AND type = 'QueryFinish') AS offsets,
    (SELECT sum(column_data_compressed_bytes) FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_nested' AND active AND column = 'n.b') AS n_b_bytes
SELECT offsets > 0 AND offsets_and_full BETWEEN offsets AND (offsets + n_b_bytes) * 1.25 AS offsets_charged_with_full_column;

-- The renamed column holds the same rows with the same `CODEC`, so it must be priced like the original.
WITH
    (SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] FROM system.query_log
     WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
        AND log_comment = '05452_codec_renamed' AND type = 'QueryFinish') AS renamed,
    (SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] FROM system.query_log
     WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
        AND log_comment = '05452_codec' AND type = 'QueryFinish') AS original
SELECT original > 0 AND renamed BETWEEN original * 0.8 AND original * 1.25 AS renamed_codec_found;

DROP TABLE t_wide;
DROP TABLE t_wide_single_stream;
DROP TABLE t_compact;
DROP TABLE t_renamed;
DROP TABLE t_nested;
DROP TABLE t_nested_compact;
DROP TABLE t_codec;
DROP TABLE t_codec_renamed;

-- A selection of unusually large values must be priced by what it reads, not by the average row of the part:
-- the first 1000 rows hold 1 KiB strings, the others empty ones, and `CODEC(NONE)` keeps their size on disk.
DROP TABLE IF EXISTS t_skew_wide;
DROP TABLE IF EXISTS t_skew_compact;

-- The compact part is compared with the sizes of the wide one, so their granules are as large, see `t_compact`.
CREATE TABLE t_skew_wide (key UInt64, flag UInt8, s String CODEC(NONE)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1,
    index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_skew_compact (key UInt64, flag UInt8, s String CODEC(NONE)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, compress_per_column_in_compact_parts = 1, ratio_of_defaults_for_sparse_serialization = 1,
    index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t_skew_wide SELECT number, number < 1000, if(number < 1000, repeat('x', 1024), '') FROM numbers(2e5);
INSERT INTO t_skew_compact SELECT * FROM t_skew_wide;

DROP TABLE IF EXISTS t_cache_sparse;
DROP TABLE IF EXISTS t_cache_dynamic;

CREATE TABLE t_cache_sparse (key UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5,
    nullable_serialization_version = 'allow_sparse', serialization_info_version = 'with_types';
CREATE TABLE t_cache_dynamic (key UInt64, d Dynamic) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_cache_sparse SELECT number, if(number % 100 = 0, number, NULL) FROM numbers(2e5);
INSERT INTO t_cache_dynamic SELECT number, if(number % 2, number::Dynamic, toString(number)::Dynamic) FROM numbers(2e5);

DROP TABLE IF EXISTS t_cache_quantized;
SET enable_quantized_codec = 1;
CREATE TABLE t_cache_quantized (key UInt64, vec Array(Float32) CODEC(Quantized('product', 8, 4, 2))) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_cache_quantized SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(2e5);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2;

SELECT s FROM t_skew_wide PREWHERE flag = 1 FORMAT Null SETTINGS log_comment = '05452_skew_wide_prewhere';
SELECT s FROM t_skew_wide WHERE key < 1000 FORMAT Null SETTINGS log_comment = '05452_skew_wide_primary_key';
SELECT s FROM t_skew_compact PREWHERE flag = 1 FORMAT Null SETTINGS log_comment = '05452_skew_compact_prewhere';
SELECT s FROM t_skew_compact WHERE key < 1000 FORMAT Null SETTINGS log_comment = '05452_skew_compact_primary_key';

-- A part with a single `LowCardinality` dictionary of long values, flushed in its own block: the marks of the granules that
-- share it are all the same, at the start of the block.
DROP TABLE IF EXISTS t_cache_lowcardinality;
CREATE TABLE t_cache_lowcardinality (key UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 1, index_granularity = 8192,
    index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_cache_lowcardinality SELECT number, hex(sipHash128(number % 5000)) || hex(sipHash128(number % 5000 + 5000)) FROM numbers(2e5);

-- Rows served from the columns cache are read from no file, and must be priced as reading them from the part would be.
SELECT key, max(s) FROM t_skew_wide GROUP BY key FORMAT Null
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_cache_wide_1';
SELECT key, max(s) FROM t_skew_wide GROUP BY key FORMAT Null
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_cache_wide_2';
SELECT key, max(s) FROM t_skew_compact GROUP BY key FORMAT Null
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_cache_compact_1';
SELECT key, max(s) FROM t_skew_compact GROUP BY key FORMAT Null
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_cache_compact_2';
SELECT lc FROM t_cache_lowcardinality WHERE key < 20000 FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_cache_lowcardinality_1';
SELECT lc FROM t_cache_lowcardinality WHERE key < 20000 FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_cache_lowcardinality_2';

-- The streams of the null map of a sparse `Nullable`, of a `Dynamic` and of a `Quantized` column are not all known without reading the column,
-- so the bytes of the granules served from the cache are not measured, and no statistics are collected.
SELECT sum(n.null) FROM t_cache_sparse FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_sparse_1';
SELECT sum(n.null) FROM t_cache_sparse FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_sparse_2';
SELECT d FROM t_cache_dynamic FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_dynamic_1';
SELECT d FROM t_cache_dynamic FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_dynamic_2';
-- A `Quantized` column lists its codes and codebook, which reading the vectors skips, and the codebook of the part is
-- one value, read from the beginning of its file for any granule.
SELECT vec FROM t_cache_quantized FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_quantized_1';
SELECT vec FROM t_cache_quantized FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_quantized_2';
SELECT vec.product_quantization_codebook FROM t_cache_quantized FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_quantized_codebook_1';
SELECT vec.product_quantization_codebook FROM t_cache_quantized FORMAT Null
SETTINGS merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1, log_comment = '05452_unknown_streams_quantized_codebook_2';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- A read is measured if the cache served nothing. Which granules it serves is not fixed: other tests can evict the
-- entries before the second read. The replicas share the cache but measure only the local reads, so the first read must
-- not serve itself: the ranges are not split into overlapping ones.
SELECT log_comment, if(ProfileEvents['ColumnsCacheHits'] = 0,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] = 0) AS measured_only_when_streams_known
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment LIKE '05452\\_unknown\\_streams\\_%' AND type = 'QueryFinish'
ORDER BY log_comment;

SELECT part_type, min(input_bytes) > 0 AND max(input_bytes) <= min(input_bytes) * 1.25 AS cache_priced_as_part
FROM
(
    SELECT extract(log_comment, '05452_cache_(\\w+)_\\d') AS part_type, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
        AND log_comment LIKE '05452\\_cache\\_%' AND type = 'QueryFinish'
)
GROUP BY part_type
ORDER BY part_type;

WITH
    (SELECT sum(column_data_compressed_bytes) FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_skew_wide' AND active) AS wide_bytes
SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] BETWEEN 1000 * 1024 * 0.8 AND wide_bytes * 1.25 AS large_values_priced
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment LIKE '05452\\_skew\\_%' AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_skew_wide;
DROP TABLE t_skew_compact;
DROP TABLE t_cache_sparse;
DROP TABLE t_cache_dynamic;
DROP TABLE t_cache_quantized;
DROP TABLE t_cache_lowcardinality;
