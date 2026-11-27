-- Names rewritten by aggregate_functions_null_for_empty (sumIf -> sumOrNullIf) are resolved again by shards and
-- parallel replicas and must not be rewritten a second time.

DROP TABLE IF EXISTS t_null_for_empty;
DROP TABLE IF EXISTS t_null_for_empty_states;
DROP TABLE IF EXISTS t_null_for_empty_double;

CREATE TABLE t_null_for_empty (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_null_for_empty SELECT number FROM numbers(10);

CREATE TABLE t_null_for_empty_states (s AggregateFunction(sumOrNull, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_for_empty_states SELECT sumOrNullState(number) FROM numbers(10);

CREATE TABLE t_null_for_empty_double (s AggregateFunction(sumOrNullOrNull, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_for_empty_double SELECT sumOrNullOrNullState(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 0;

SET aggregate_functions_null_for_empty = 1;

SELECT toTypeName(sumState(number)), toTypeName(sumOrNullState(number)) FROM numbers(1);

-- States stored as sumOrNullOrNull (an explicit sumOrNullState under the setting before) stay readable and writable.
SELECT sumOrNullMerge(s) FROM t_null_for_empty_double;
INSERT INTO t_null_for_empty_double SELECT sumOrNullOrNullState(number) FROM numbers(10);

SET prefer_localhost_replica = 0;

SELECT sum(number) FROM remote('127.0.0.1,127.0.0.1', numbers(10));
SELECT sumIf(number, number > 5), sumIf(number, number > 100) FROM remote('127.0.0.1,127.0.0.1', numbers(10));
SELECT sumOrNullIf(number, number > 5) FROM remote('127.0.0.1,127.0.0.1', numbers(10));
SELECT finalizeAggregation(sumState(number)) FROM remote('127.0.0.1,127.0.0.1', numbers(10));
SELECT sumMerge(s), finalizeAggregation(sumMergeState(s)) FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_null_for_empty_states);
SELECT sumOrNullMerge(s), finalizeAggregation(sumOrNullMergeState(s)) FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_null_for_empty_double);
SELECT k % 2 AS g, sumIf(k, k > 2) FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_null_for_empty) GROUP BY g ORDER BY g;

-- Without the local plan every parallel replica is a remote one.
SET enable_parallel_replicas = 2, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_plan_based = 0;

SELECT sumIf(k, k % 3 = 0) FROM t_null_for_empty;
SELECT finalizeAggregation(sumState(k)) FROM t_null_for_empty;
SELECT sumMerge(s) FROM t_null_for_empty_states;
SELECT sumOrNullMerge(s) FROM t_null_for_empty_double;

DROP TABLE t_null_for_empty;
DROP TABLE t_null_for_empty_states;
DROP TABLE t_null_for_empty_double;
