-- The plan with parallel replicas that the automatic mode builds to decide is optimized without primary-key analysis:
-- its reads take the analysis of the single-node plan. Join optimization estimates every input of a join, and for a
-- read without an analysis that estimate used to analyze it, a second time and unpruned, so the probe checked every
-- mark of both tables against the primary key once more.

SET enable_analyzer=1;

-- Mode 2 builds the plan with parallel replicas on every run, while mode 1 would decide each query after the first
-- from the statistics the first one collected, without building it.
SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET query_plan_optimize_join_order_randomize=0;
-- Don't let the min-bytes gate skip the small reads before the plan with parallel replicas is built.
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET use_query_condition_cache=0;
-- With a throwing read limit, which the test profile sets, a join estimate analyzes without keeping the result,
-- so the single-node plan analyzes every read again later, and there is no analysis for the probe to take.
SET max_rows_to_read=0;

CREATE TABLE fact (k UInt64, d UInt32, v Float64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dim (d UInt32, name String) ENGINE = MergeTree ORDER BY d;
INSERT INTO fact SELECT number, number % 1000, number / 7 FROM numbers(1000000);
INSERT INTO dim SELECT number, 'n' || toString(number) FROM numbers(1000);

SELECT dim.name, sum(fact.v) FROM fact INNER JOIN dim ON fact.d = dim.d
WHERE fact.k BETWEEN 100000 AND 110000 AND dim.d < 100
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_join_auto';

SELECT dim.name, sum(fact.v) FROM fact INNER JOIN dim ON fact.d = dim.d
WHERE fact.k BETWEEN 100000 AND 110000 AND dim.d < 100
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_join_off', enable_parallel_replicas = 0;

SYSTEM FLUSH LOGS query_log;

-- The query is compared with the same query without automatic parallel replicas.
SELECT
    q.ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS plan_built,
    q.ProfileEvents['IndexAnalysisRounds'] = base.ProfileEvents['IndexAnalysisRounds'] AS same_index_analysis_rounds,
    q.ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks'] = base.ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks']
        AS same_marks_checked
FROM system.query_log AS q, system.query_log AS base
WHERE q.current_database = currentDatabase() AND base.current_database = currentDatabase()
    AND q.type = 'QueryFinish' AND base.type = 'QueryFinish' AND q.event_date >= yesterday() AND base.event_date >= yesterday()
    AND q.query_id = q.initial_query_id AND base.query_id = base.initial_query_id
    AND q.log_comment = '05508_join_auto' AND base.log_comment = '05508_join_off';
