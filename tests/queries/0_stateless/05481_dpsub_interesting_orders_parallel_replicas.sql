-- A join the optimizer chose to run as `full_sorting_merge` stays one after plan-based parallel replicas clone and ship
-- the plan: the decision must survive `JoinStepLogical::clone` and plan serialization, or the runtime filter turns it into a hash join.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET enable_join_transitive_predicates = 1;
SET optimize_read_in_order = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET enable_join_runtime_filters = 1;
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;
-- A merge join of sorted inputs is costed as sharded by primary key ranges only with this on.
SET query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE IF EXISTS pr_sorted1;
DROP TABLE IF EXISTS pr_sorted2;

CREATE TABLE pr_sorted1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE pr_sorted2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO pr_sorted1 SELECT number, number FROM numbers(1000);
INSERT INTO pr_sorted2 SELECT number, number FROM numbers(1000);

-- The plan is chosen for the sizes of the hint: large enough for a sort-merge join.
SET param__internal_join_table_stat_hints = '{
    "pr_sorted1": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "pr_sorted2": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } }
}';

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;
-- Pin the manual mode, so that the query is distributed whatever the randomized automatic mode decides.
SET automatic_parallel_replicas_mode = 0;

SELECT count(), sum(pr_sorted1.v + pr_sorted2.v) FROM pr_sorted1 JOIN pr_sorted2 ON pr_sorted1.k = pr_sorted2.k
SETTINGS log_comment = 'dpsub_pr_merge';

SYSTEM FLUSH LOGS query_log;

-- The initiator and the replicas run the query under the same comment; all of them join with `full_sorting_merge` only.
SELECT
    countIf(has(used_join_algorithms, 'FULL_SORTING_MERGE')) > 0,
    countIf(arrayExists(algorithm -> algorithm != 'FULL_SORTING_MERGE', used_join_algorithms)) = 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
    AND type = 'QueryFinish' AND log_comment = 'dpsub_pr_merge' AND current_database = currentDatabase();

DROP TABLE pr_sorted1;
DROP TABLE pr_sorted2;
