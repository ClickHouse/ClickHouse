-- Tags: no-parallel-replicas
-- The filter expression `multiIf(v > 0, v, 0)` is rewritten to `if(v > 0, v, 0)` by `optimize_multiif_to_if`.
-- The original form of the key wraps it only into the second key column, the rewritten form into the first one.
-- Both must be used, so the leading key column constrains the read.

SET explain_query_plan_default = 'legacy';
SET optimize_multiif_to_if = 1, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_mixed_wrapping;

CREATE TABLE t_mixed_wrapping (v Int32, id UInt32)
ENGINE = MergeTree
ORDER BY (toInt64(multiIf(v > 0, v, 0)), toFloat64(if(v > 0, v, 0)))
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_mixed_wrapping SELECT number, number FROM numbers(100);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_mixed_wrapping WHERE multiIf(v > 0, v, 0) > 97) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_mixed_wrapping WHERE multiIf(v > 0, v, 0) > 97;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_mixed_wrapping WHERE multiIf(v > 0, v, 0) > 97 SETTINGS analyze_index_with_multiple_key_columns_per_condition = 0) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_mixed_wrapping WHERE multiIf(v > 0, v, 0) > 97 SETTINGS analyze_index_with_multiple_key_columns_per_condition = 0;

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_mixed_wrapping WHERE multiIf(v > 0, v, 0) = 98) WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_mixed_wrapping WHERE multiIf(v > 0, v, 0) = 98;

DROP TABLE t_mixed_wrapping;
