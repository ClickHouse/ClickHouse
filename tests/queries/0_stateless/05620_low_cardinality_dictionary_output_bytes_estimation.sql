-- Aggregation by dictionary index emits keys that share the source dictionary, and
-- `ColumnLowCardinality::byteSize` does not count a shared dictionary. The runtime dataflow statistics
-- price the shipping term of the automatic parallel replicas plan from the output bytes, so they must
-- still count the key values the result references, as they do for keys aggregated by value.

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET use_query_condition_cache=0, use_columns_cache=0;
SET max_rows_to_group_by=0;
SET query_plan_enable_optimizations=1, optimize_aggregation_in_order=0;
SET max_threads=4, group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0;

DROP TABLE IF EXISTS lc_output_bytes_dictionary;
DROP TABLE IF EXISTS lc_output_bytes_value;

-- 1000 distinct keys of 1 KiB of hexadecimal hashes each: the keys dominate the output and compress
-- poorly, so a result that leaves out their values falls orders of magnitude below the control.
CREATE TABLE lc_output_bytes_dictionary (id UInt64, s LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

CREATE TABLE lc_output_bytes_value (id UInt64, s String)
ENGINE = MergeTree ORDER BY id;

INSERT INTO lc_output_bytes_dictionary
SELECT number, arrayStringConcat(arrayMap(i -> hex(sipHash128(toString(number % 1000 * 32 + i))), range(32)))
FROM numbers(100000)
SETTINGS low_cardinality_use_single_dictionary_for_part = 1;

INSERT INTO lc_output_bytes_value SELECT id, s FROM lc_output_bytes_dictionary;

SELECT s, sum(id) FROM lc_output_bytes_dictionary GROUP BY s FORMAT Null SETTINGS log_comment = 'lc_output_bytes/dictionary';
SELECT s, sum(id) FROM lc_output_bytes_value GROUP BY s FORMAT Null SETTINGS log_comment = 'lc_output_bytes/value';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

DROP TABLE lc_output_bytes_dictionary;
DROP TABLE lc_output_bytes_value;

SYSTEM FLUSH LOGS query_log;

-- The exact sizes depend on the build, so only their ratio is checked.
SELECT
    dictionary_queries,
    value_queries,
    dictionary_bytes > 0 AND value_bytes > 0
        AND greatest(dictionary_bytes, value_bytes) / least(dictionary_bytes, value_bytes) <= 2 AS comparable
FROM
(
    SELECT
        countIf(log_comment = 'lc_output_bytes/dictionary') AS dictionary_queries,
        countIf(log_comment = 'lc_output_bytes/value') AS value_queries,
        sumIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = 'lc_output_bytes/dictionary') AS dictionary_bytes,
        sumIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = 'lc_output_bytes/value') AS value_bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE
      AND current_database = currentDatabase() AND log_comment LIKE 'lc_output_bytes/%' AND type = 'QueryFinish'
)
FORMAT Vertical;
