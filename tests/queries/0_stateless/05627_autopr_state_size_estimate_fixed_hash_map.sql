-- Automatic parallel replicas estimates the size of the aggregate states from a sample of them.
-- A `UInt16` key is aggregated in a fixed hash map, which is iterated in key order. The sample has to
-- span the whole table: here the states of the first 10000 keys are empty and all the others are large,
-- so a sample taken from a prefix of the table underestimates the states by two orders of magnitude.

DROP TABLE IF EXISTS t;

CREATE TABLE t (k UInt16, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t SELECT number, if(number < 10000, '', repeat(hex(SHA256(toString(number))), 4)) FROM numbers(65536);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

SELECT k, any(s) FROM t GROUP BY k FORMAT Null SETTINGS log_comment='05627_autopr_state_size_estimate_fixed_hash_map';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- The states take 14217216 bytes uncompressed.
SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e6 AND 3e7
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05627_autopr_state_size_estimate_fixed_hash_map') AND (type = 'QueryFinish');

DROP TABLE t;
