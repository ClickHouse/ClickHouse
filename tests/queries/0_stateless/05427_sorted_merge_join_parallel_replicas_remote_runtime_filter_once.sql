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

-- Every remote replica of the fragment builds the filter of the only key in one step, as with plain `hash`.
SELECT initial.log_comment, count() > 0, groupUniqArray(steps)
FROM
(
    SELECT q.initial_query_id AS initial_query_id, uniqExactIf(p.step_uniq_id, p.name = 'BuildRuntimeFilterTransform') AS steps
    FROM system.query_log AS q
    LEFT JOIN system.processors_profile_log AS p ON p.query_id = q.query_id AND p.event_date >= yesterday()
    WHERE q.type = 'QueryFinish' AND NOT q.is_initial_query AND q.event_date >= yesterday()
    GROUP BY q.initial_query_id, q.query_id
) AS secondary
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05427_%' AND event_date >= yesterday()
) AS initial ON initial.query_id = secondary.initial_query_id
GROUP BY initial.log_comment
ORDER BY initial.log_comment
SETTINGS enable_parallel_replicas = 0;

DROP TABLE smjrrf_fact SYNC;
DROP TABLE smjrrf_dim SYNC;
