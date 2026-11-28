-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel-replicas: the part aggregation cache is intentionally not used for parallel-replica reads

-- Regression test: the cache population must evaluate the replayed expressions (`WHERE`,
-- `PREWHERE`, aggregate arguments) with the query's `ExpressionActionsSettings`, so that
-- short-circuit evaluation and the temporary-column limits apply exactly as in the query itself.

-- See `05029_part_aggregation_cache_default_expression` for why the limits are pinned to 0.
SET allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache_actions_settings;

CREATE TABLE t_part_agg_cache_actions_settings (x UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_part_agg_cache_actions_settings;
INSERT INTO t_part_agg_cache_actions_settings VALUES (0, 10), (1, 20), (2, 30);

-- `max_temporary_columns` must be enforced on the cache population as well: the query must fail
-- with and without the cache, and must not produce a cache entry.
SELECT x % 1 AS k, sum(x + v + 1) FROM t_part_agg_cache_actions_settings GROUP BY k SETTINGS max_temporary_columns = 2, allow_experimental_part_aggregation_cache = 0; -- { serverError TOO_MANY_TEMPORARY_COLUMNS }
SELECT x % 1 AS k, sum(x + v + 1) FROM t_part_agg_cache_actions_settings GROUP BY k SETTINGS max_temporary_columns = 2; -- { serverError TOO_MANY_TEMPORARY_COLUMNS }
SELECT count() FROM system.part_aggregation_cache;

-- With short-circuit evaluation the division by zero is never evaluated, so the population
-- succeeds and the cached state is reused.
SELECT x % 1 AS k, sum(if(x = 0, 0, intDiv(42, x))) FROM t_part_agg_cache_actions_settings GROUP BY k SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT count() > 0 FROM system.part_aggregation_cache;
SELECT x % 1 AS k, sum(if(x = 0, 0, intDiv(42, x))) FROM t_part_agg_cache_actions_settings GROUP BY k SETTINGS short_circuit_function_evaluation = 'force_enable';

-- The same in `WHERE`.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT x % 1 AS k, count() FROM t_part_agg_cache_actions_settings WHERE if(x = 0, 1, intDiv(42, x)) > 0 GROUP BY k SETTINGS short_circuit_function_evaluation = 'force_enable', optimize_move_to_prewhere = 0;
SELECT count() > 0 FROM system.part_aggregation_cache;

-- The same in `PREWHERE`.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT x % 1 AS k, count() FROM t_part_agg_cache_actions_settings PREWHERE if(x = 0, 1, intDiv(42, x)) > 0 GROUP BY k SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT count() > 0 FROM system.part_aggregation_cache;

DROP TABLE t_part_agg_cache_actions_settings;
