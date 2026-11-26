-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN then also shows the replicas' part of the plan, planned without the outer LIMIT.

-- Join runtime filters are not planned below a `LIMIT 0` that takes no row: no probe row is ever read there.

SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;

CREATE TABLE t_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_build (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_probe SELECT number, number FROM numbers(10000);
INSERT INTO t_build SELECT number * 2, number FROM numbers(5);

SELECT 'a: LIMIT 0';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT * FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k) LIMIT 0)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'b: LIMIT 1';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT * FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k) LIMIT 1)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'c: LIMIT 0 OFFSET 1';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT * FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k) LIMIT 0 OFFSET 1)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'd: LIMIT 0, exact_rows_before_limit';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT * FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k) LIMIT 0
    SETTINGS exact_rows_before_limit = 1)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'e: WITH TOTALS LIMIT 0';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT w, count() FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k)
    GROUP BY w WITH TOTALS LIMIT 0)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'f: LIMIT 0 inside a join side';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM t_probe AS p INNER JOIN
    (SELECT q.k AS k FROM t_probe AS q INNER JOIN t_build AS b USING k LIMIT 0) AS s USING k)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'g: LIMIT 0 in one UNION ALL branch';
SELECT trimLeft(explain) FROM (EXPLAIN
    (SELECT p.v FROM t_probe AS p INNER JOIN t_build AS b USING k LIMIT 0)
    UNION ALL
    (SELECT p.v FROM t_probe AS p INNER JOIN t_build AS b USING k))
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'i: GROUP BY LIMIT 0';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT w, count() FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k)
    GROUP BY w LIMIT 0)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'j: WITH TOTALS ORDER BY LIMIT 0';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT w, count() FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k)
    GROUP BY w WITH TOTALS ORDER BY w LIMIT 0)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'k: LIMIT 0, extremes';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT * FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k) LIMIT 0
    SETTINGS extremes = 1)
WHERE explain LIKE '%BuildRuntimeFilter%';

SELECT 'h: results';
SELECT count() FROM (SELECT * FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k) LIMIT 0);
SELECT count() FROM (SELECT p.v, b.w FROM t_probe AS p INNER JOIN t_build AS b USING k);

DROP TABLE t_probe;
DROP TABLE t_build;
