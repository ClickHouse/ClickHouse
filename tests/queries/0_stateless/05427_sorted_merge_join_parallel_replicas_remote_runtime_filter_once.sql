-- Companion of 05366_sorted_merge_join_parallel_replicas_fragment_runtime_filter for the remote replicas.
-- The join ships inside the parallel replicas fragment with the suppression flag of the runtime filter
-- pass set. On a remote replica the coordinated input is not read in the order of the join keys, so the
-- first traversal of the optimizer restores the runtime filter and the selection falls through to `hash`.
-- The flag has to be consumed then: otherwise the re-run of the pass after `applyParallelReplicas`
-- plants a second `BuildRuntimeFilterStep` for every key. See PR #112973 review.

DROP TABLE IF EXISTS smjrrf_fact SYNC;
DROP TABLE IF EXISTS smjrrf_dim SYNC;

CREATE TABLE smjrrf_fact (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/smjrrf_fact', 'r1') ORDER BY id;
CREATE TABLE smjrrf_dim (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/smjrrf_dim', 'r1') ORDER BY id;
INSERT INTO smjrrf_fact SELECT number % 300000, number FROM numbers(400000);
INSERT INTO smjrrf_dim SELECT number % 200000, number * 2 FROM numbers(500000);

SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 0;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

SET log_processors_profiles = 1, log_queries = 1;

SELECT count(), sum(f.x + d.v) FROM smjrrf_fact AS f INNER JOIN smjrrf_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4, log_comment = '05427_sorted_merge_hash';
SELECT count(), sum(f.x + d.v) FROM smjrrf_fact AS f INNER JOIN smjrrf_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'hash', max_threads = 4, log_comment = '05427_hash';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- No remote replica of the fragment builds the filter of the only key in more than one step, as with plain `hash`.
-- A remote replica may not take part in the query at all under load (it is cancelled once the others have read
-- everything), so the check is that no replica has more than one step, not that every replica has exactly one.
-- The log comments are matched exactly: the test runner sets the file name as the default `log_comment`.
WITH
    initial AS
    (
        SELECT query_id, log_comment
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
            AND log_comment IN ('05427_hash', '05427_sorted_merge_hash')
    ),
    secondary AS
    (
        SELECT query_id, initial_query_id
        FROM system.query_log
        WHERE type = 'QueryFinish' AND NOT is_initial_query AND event_date >= yesterday()
            AND initial_query_id IN (SELECT query_id FROM initial)
    ),
    steps AS
    (
        SELECT query_id, uniqExact(step_uniq_id) AS steps
        FROM system.processors_profile_log
        WHERE name = 'BuildRuntimeFilterTransform' AND event_date >= yesterday()
            AND query_id IN (SELECT query_id FROM secondary)
        GROUP BY query_id
    )
SELECT i.log_comment, max(st.steps) <= 1
FROM initial AS i
LEFT JOIN secondary AS s ON s.initial_query_id = i.query_id
LEFT JOIN steps AS st ON st.query_id = s.query_id
GROUP BY i.log_comment
ORDER BY i.log_comment
SETTINGS enable_parallel_replicas = 0, join_use_nulls = 0;

DROP TABLE smjrrf_fact SYNC;
DROP TABLE smjrrf_dim SYNC;
