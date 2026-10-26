-- The analyzer turns `length(s)` and `s <> ''` into reads of the subcolumn `s.size`. The input bytes that
-- automatic parallel replicas records for such a read must be priced by what is read from disk for `s.size`.
-- They used to inherit the compression ratio of the other columns read next to it: here the key compresses
-- several hundred times better than the sizes, so the recorded input was far below the bytes on disk.

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_wide_single_stream;
DROP TABLE IF EXISTS t_compact;

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

INSERT INTO t_wide SELECT intDiv(number, 100000), repeat('x', cityHash64(number) % 50) FROM numbers(1e6);
INSERT INTO t_wide_single_stream SELECT * FROM t_wide;
INSERT INTO t_compact SELECT * FROM t_wide;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_functions_to_subcolumns = 1;

SELECT key, avg(length(s)) FROM t_wide WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_wide';
SELECT key, avg(length(s)) FROM t_wide_single_stream WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_wide_single_stream';
SELECT key, avg(length(s)) FROM t_compact WHERE s <> '' GROUP BY key FORMAT Null SETTINGS log_comment = '05452_compact';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- The bytes on disk of `key` and of what is read for `s.size`: its own stream if it has one, otherwise all of `s`.
-- A compact part reports no per-column sizes; it holds the same rows as `t_wide`, so it is compared with those.
WITH on_disk AS
(
    SELECT table, sum(if(column = 'key' OR NOT has(subcolumns.names, 'size'), column_data_compressed_bytes,
        subcolumns.data_compressed_bytes[indexOf(subcolumns.names, 'size')])) AS bytes
    FROM system.parts_columns
    WHERE database = currentDatabase() AND active
    GROUP BY table
)
SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] BETWEEN on_disk.bytes / 2 AND on_disk.bytes * 2 AS input_bytes_match_disk
FROM system.query_log
JOIN on_disk ON on_disk.table = if(log_comment = '05452_wide_single_stream', 't_wide_single_stream', 't_wide')
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE AND current_database = currentDatabase()
    AND log_comment IN ('05452_wide', '05452_wide_single_stream', '05452_compact') AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_wide;
DROP TABLE t_wide_single_stream;
DROP TABLE t_compact;
