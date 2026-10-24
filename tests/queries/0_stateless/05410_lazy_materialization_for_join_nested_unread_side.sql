-- Lazy materialization over joins with a join that reads nothing of a side, above another join that reads a column of
-- that side for its condition. The outer join keeps a column of the side for the number of rows - its first input, not
-- the one the inner join reads - so that column stays in the main read rather than being read lazily. The results are
-- compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in, and the comma join stays a cross join.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET cross_to_inner_join_rewrite = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS r;

CREATE TABLE l (x UInt64, y UInt64, payload String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t (y UInt64) ENGINE = MergeTree ORDER BY y;
CREATE TABLE r (payload String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO l VALUES (1, 10, 'a'), (2, 20, 'b');
INSERT INTO t VALUES (10), (20);
INSERT INTO r VALUES ('r1'), ('r2');

SELECT '-- without ORDER BY: which rows the LIMIT keeps is not known, so the result checks what holds for any of them';
SELECT count(), min(y = x * 10), min(payload = if(x = 1, 'a', 'b')), min(rp IN ('r1', 'r2')) FROM (
    SELECT l.x AS x, l.y AS y, l.payload AS payload, r.payload AS rp FROM l INNER JOIN t ON l.y = t.y CROSS JOIN r LIMIT 2)
SETTINGS query_plan_lazy_materialization_for_join = 1;

SELECT '-- with ORDER BY';
SELECT l.x, l.y, l.payload, r.payload FROM l INNER JOIN t ON l.y = t.y CROSS JOIN r ORDER BY r.payload, l.y LIMIT 3
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.x, l.y, l.payload, r.payload FROM l INNER JOIN t ON l.y = t.y CROSS JOIN r ORDER BY r.payload, l.y LIMIT 3
SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- a column is still read lazily';
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.x, l.y, l.payload, r.payload FROM l INNER JOIN t ON l.y = t.y CROSS JOIN r ORDER BY r.payload, l.y LIMIT 3);

DROP TABLE l;
DROP TABLE t;
DROP TABLE r;
