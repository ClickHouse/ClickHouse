-- Lazy materialization over joins with a random value and a value computed from it, on the side of an outer join: the
-- random value is computed once, where the plan computes it, whichever of the two the query lists first, so the two
-- agree on every row. One of them crossing the LIMIT must not leave the other to be drawn again above it.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id UInt64, k UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t1 SELECT number, number FROM numbers(10);
INSERT INTO t2 SELECT number, concat('p', toString(number)) FROM numbers(10);

SELECT '-- the random value first';
SELECT id, r = toUInt32(s), payload FROM (
    SELECT t1.id, x.r, x.s, x.payload FROM t1 LEFT JOIN (SELECT k, rand() AS r, CAST(r AS String) AS s, payload FROM t2) AS x ON t1.k = x.k
    ORDER BY t1.id LIMIT 3);

SELECT '-- the value computed from it first';
SELECT id, r = toUInt32(s), payload FROM (
    SELECT t1.id, x.s, x.r, x.payload FROM t1 LEFT JOIN (SELECT k, rand() AS r, CAST(r AS String) AS s, payload FROM t2) AS x ON t1.k = x.k
    ORDER BY t1.id LIMIT 3);

SELECT '-- the payload is read lazily';
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT t1.id, x.r, x.s, x.payload FROM t1 LEFT JOIN (SELECT k, rand() AS r, CAST(r AS String) AS s, payload FROM t2) AS x ON t1.k = x.k
    ORDER BY t1.id LIMIT 3);

DROP TABLE t1;
DROP TABLE t2;
