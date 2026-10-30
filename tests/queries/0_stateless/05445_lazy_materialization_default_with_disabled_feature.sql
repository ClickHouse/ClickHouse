-- The read fills a missing column from its `DEFAULT` with the settings of the features turned on, so the default works
-- when the query has the setting it was created with off. Lazy materialization looks at the default in the same way.

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS r;

CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number FROM numbers(1000);
INSERT INTO r SELECT number, toString(number) FROM numbers(1000);

SET allow_fuzz_query_functions = 1;
ALTER TABLE t ADD COLUMN s String DEFAULT fuzzQuery('SELECT 1');
SET allow_fuzz_query_functions = 0;

SELECT k, s != '' FROM t ORDER BY k DESC LIMIT 3;
SELECT t.k, t.s != '', r.payload FROM t JOIN r ON t.k = r.k ORDER BY t.k DESC LIMIT 3
SETTINGS query_plan_lazy_materialization_for_join = 1;

DROP TABLE t;
DROP TABLE r;
