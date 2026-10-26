-- The analyzer turns `length(s)` and `s <> ''` into reads of the subcolumn `s.size`. The input bytes that
-- automatic parallel replicas records for such a read must be priced by what is read from disk for `s.size`.
-- They used to inherit the compression ratio of the other columns read next to it: here the key compresses
-- several hundred times better than the sizes, so the recorded input was far below the bytes on disk.

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_wide_single_stream;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_renamed;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_codec;
DROP TABLE IF EXISTS t_codec_renamed;

-- `s.size` has a stream of its own.
CREATE TABLE t_wide (key UInt64, s String) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'with_size_stream',
    serialization_info_version = 'with_types', ratio_of_defaults_for_sparse_serialization = 1;

-- The sizes are kept inside the only stream of `s`, so reading `s.size` reads all of `s`.
CREATE TABLE t_wide_single_stream (key UInt64, s String) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, string_serialization_version = 'single_stream',
    ratio_of_defaults_for_sparse_serialization = 1;

CREATE TABLE t_compact (key UInt64, s String) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, string_serialization_version = 'with_size_stream',
    serialization_info_version = 'with_types', ratio_of_defaults_for_sparse_serialization = 1;

-- Renamed while the mutation that renames the files in the part is held back: the part still names the column `s`.
CREATE TABLE t_renamed AS t_wide;

-- `length(n.a)` reads the offsets `n.size0` that `n.a` shares with `n.b`.
CREATE TABLE t_nested (key UInt64, n Nested(a String, b UInt64)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1, ratio_of_defaults_for_sparse_serialization = 1;

-- A compact part is sampled with the `CODEC` of the column, which must be found under its old name too.
CREATE TABLE t_codec (key UInt64, s String CODEC(NONE)) ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_codec_renamed AS t_codec;

INSERT INTO t_wide SELECT intDiv(number, 100000), repeat('x', cityHash64(number) % 50) FROM numbers(1e6);
INSERT INTO t_wide_single_stream SELECT * FROM t_wide;
INSERT INTO t_compact SELECT * FROM t_wide;
INSERT INTO t_renamed SELECT * FROM t_wide;
INSERT INTO t_nested SELECT intDiv(number, 100000), arrayMap(i -> 'x', range(cityHash64(number) % 5)), arrayMap(i -> i, range(cityHash64(number) % 5))
FROM numbers(1e6);

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
-- Some cases are compared with `ReadCompressedBytes`, which a read served from the uncompressed cache does not count.
SET use_uncompressed_cache = 0;

SELECT key, avg(length(s)) FROM t_wide WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_wide';
SELECT key, avg(length(s)) FROM t_wide_single_stream WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_wide_single_stream';
SELECT key, avg(length(s)) FROM t_compact WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_compact';
SELECT key, avg(length(renamed)) FROM t_renamed WHERE renamed <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_renamed';
SELECT key, avg(length(n.a)) FROM t_nested GROUP BY key FORMAT Null SETTINGS log_comment = '05452_nested';
-- Both read the one stream `n.size0`.
SELECT key, avg(length(n.a)), avg(length(n.b)) FROM t_nested GROUP BY key FORMAT Null SETTINGS log_comment = '05452_nested_shared_stream';
SELECT key, max(s) FROM t_codec GROUP BY key FORMAT Null SETTINGS log_comment = '05452_codec';
SELECT key, max(renamed) FROM t_codec_renamed GROUP BY key FORMAT Null SETTINGS log_comment = '05452_codec_renamed';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- The bytes on disk of `key` and of what is read for `s.size`: its own stream if it has one, otherwise all of `s`.
-- A compact part reports no per-column sizes; it holds the same rows as `t_wide`, so it is compared with those,
-- and so is the renamed table, whose part is still the one written for `t_wide`'s columns.
WITH on_disk AS
(
    SELECT table, sum(if(column = 'key' OR NOT has(subcolumns.names, 'size'), column_data_compressed_bytes,
        subcolumns.data_compressed_bytes[indexOf(subcolumns.names, 'size')])) AS bytes
    FROM system.parts_columns
    WHERE database = currentDatabase() AND active AND table IN ('t_wide', 't_wide_single_stream')
    GROUP BY table
)
SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] BETWEEN on_disk.bytes / 2 AND on_disk.bytes * 2 AS input_bytes_match_disk
FROM system.query_log
JOIN on_disk ON on_disk.table = if(log_comment = '05452_wide_single_stream', 't_wide_single_stream', 't_wide')
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment IN ('05452_wide', '05452_wide_single_stream', '05452_compact', '05452_renamed') AND type = 'QueryFinish'
ORDER BY log_comment;

-- `n.size0` is not a column of the part, so it is compared with the bytes read from disk instead. Those are exact
-- here, so the bound is tight enough to tell a stream charged once from one charged for each column that reads it.
SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] BETWEEN ProfileEvents['ReadCompressedBytes'] * 0.8 AND ProfileEvents['ReadCompressedBytes'] * 1.25 AS input_bytes_match_disk
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment IN ('05452_nested', '05452_nested_shared_stream') AND type = 'QueryFinish'
ORDER BY log_comment;

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
DROP TABLE t_codec;
DROP TABLE t_codec_renamed;
