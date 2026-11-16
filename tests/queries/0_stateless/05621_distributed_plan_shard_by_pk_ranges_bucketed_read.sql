-- `query_plan_join_shard_by_pk_ranges` re-split the reads of a join into primary-key layers on the
-- worker of a distributed plan and took the ranges of the whole table, so every bucket of the
-- parallel (probe) side of a broadcast join read all rows and the result was multiplied by the number of buckets.

DROP TABLE IF EXISTS t_shard_by_pk_l;
DROP TABLE IF EXISTS t_shard_by_pk_r;
CREATE TABLE t_shard_by_pk_l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 339;
CREATE TABLE t_shard_by_pk_r (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 339;
INSERT INTO t_shard_by_pk_l SELECT number, number * 10 FROM numbers(1000);
INSERT INTO t_shard_by_pk_r SELECT number, number * 100 FROM numbers(500);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_shard_by_pk_ranges = 1;
-- This seed gives both sides large estimates, so the optimizer chooses a broadcast merge join
-- of a parallel read with a replicated read, which is the plan affected by the bug.
SET query_plan_optimize_join_order_randomize = 13754;
SET make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
    distributed_plan_workers_num = 3, distributed_plan_fallback_to_local_execution = 0;

SELECT count(), sum(l.v + r.w) FROM t_shard_by_pk_l AS l JOIN t_shard_by_pk_r AS r ON l.k = r.k;

DROP TABLE t_shard_by_pk_l;
DROP TABLE t_shard_by_pk_r;
