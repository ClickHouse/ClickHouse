-- The streams of the regions of a narrow `v2` read are opened by the prefixes, after the reader got the registry of the
-- read ranges: the statistics of automatic parallel replicas must count their bytes, as they do for the plain layout.

-- These caches serve repeated reads from memory, which lowers `ReadCompressedBytes` below the estimate.
SET use_uncompressed_cache = 0, use_columns_cache = 0;
-- Without the cached streams of the subcolumn, only the header is known before the prefixes are read.
SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 2, cluster_for_parallel_replicas = 'parallel_replicas';

DROP TABLE IF EXISTS t_dataflow_v1;
DROP TABLE IF EXISTS t_dataflow_v2;
CREATE TABLE t_dataflow_v1 (json JSON(SHARED a String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, object_typed_shared_data_serialization_version = 'v1';
CREATE TABLE t_dataflow_v2 (json JSON(SHARED a String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, object_typed_shared_data_serialization_version = 'v2';

INSERT INTO t_dataflow_v1 SELECT concat('{"a":"', hex(randomString(128)), '"}') FROM numbers(50000);
INSERT INTO t_dataflow_v2 SELECT concat('{"a":"', hex(randomString(128)), '"}') FROM numbers(50000);

-- The values themselves, not a function a randomized `optimize_functions_to_subcolumns` turns into a read of sizes.
SELECT json.a FROM t_dataflow_v1 FORMAT Null SETTINGS log_comment = '05895_v1';
SELECT json.a FROM t_dataflow_v2 FORMAT Null SETTINGS log_comment = '05895_v2';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, greatest(compressed_bytes, input_bytes) / least(compressed_bytes, input_bytes) <= 2
FROM (
    SELECT
        log_comment,
        ProfileEvents['ReadCompressedBytes'] AS compressed_bytes,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] AS input_bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment IN ('05895_v1', '05895_v2')
        AND type = 'QueryFinish'
)
ORDER BY log_comment;

DROP TABLE t_dataflow_v1;
DROP TABLE t_dataflow_v2;
