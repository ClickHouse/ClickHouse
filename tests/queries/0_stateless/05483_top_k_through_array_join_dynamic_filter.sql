-- Tags: no-parallel-replicas
-- no-parallel-replicas: the rewrite is skipped with parallel replicas

DROP TABLE IF EXISTS t_aj_topk;

CREATE TABLE t_aj_topk (k UInt64, arr Array(UInt32), arr2 Array(UInt32))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_aj_topk SELECT sipHash64(number), range(number % 4), range(number % 3) FROM numbers(1000);

SET optimize_read_in_order = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_top_k_through_array_join = 1;

SELECT '-- the moved sort gets the top-K filter';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, arr FROM t_aj_topk ARRAY JOIN arr ORDER BY k LIMIT 10)
WHERE explain LIKE '%__topKFilter%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, arr FROM t_aj_topk LEFT ARRAY JOIN arr ORDER BY k DESC LIMIT 5 OFFSET 3)
WHERE explain LIKE '%__topKFilter%';

SELECT '-- same rows as without the rewrite';
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr ORDER BY k LIMIT 10))
    = (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr ORDER BY k LIMIT 10 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk LEFT ARRAY JOIN arr ORDER BY k DESC LIMIT 5 OFFSET 3))
    = (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk LEFT ARRAY JOIN arr ORDER BY k DESC LIMIT 5 OFFSET 3 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr ARRAY JOIN arr2 ORDER BY k LIMIT 10))
    = (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr ARRAY JOIN arr2 ORDER BY k LIMIT 10 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr AS x LEFT ARRAY JOIN arr2 AS y ORDER BY k LIMIT 10))
    = (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr AS x LEFT ARRAY JOIN arr2 AS y ORDER BY k LIMIT 10 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk LEFT ARRAY JOIN arr AS x LEFT ARRAY JOIN range(x) AS y ORDER BY k LIMIT 10))
    = (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk LEFT ARRAY JOIN arr AS x LEFT ARRAY JOIN range(x) AS y ORDER BY k LIMIT 10 SETTINGS query_plan_top_k_through_array_join = 0));
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr AS x ARRAY JOIN range(x) AS y ORDER BY k LIMIT 10))
    = (SELECT groupArray(k) FROM (SELECT k FROM t_aj_topk ARRAY JOIN arr AS x ARRAY JOIN range(x) AS y ORDER BY k LIMIT 10 SETTINGS query_plan_top_k_through_array_join = 0));

DROP TABLE t_aj_topk;
