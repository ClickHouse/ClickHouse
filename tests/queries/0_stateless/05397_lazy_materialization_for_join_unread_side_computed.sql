-- Lazy materialization over joins with a join that reads nothing of a side: it keeps the first column of that side for
-- the number of rows, and that column can be a constant, which reads no column of a table, or a value computed from
-- the columns of several tables, which have to stay in the main read. Every query is followed by the number of lazy
-- reads in its plan, and the results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in, and a comma join stays a cross join.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET cross_to_inner_join_rewrite = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ul;
DROP TABLE IF EXISTS ur1;
DROP TABLE IF EXISTS ur2;

CREATE TABLE ul (heavy String, k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ur1 (rheavy String, k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ur2 (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO ul SELECT concat('l', toString(number)), number FROM numbers(1000);
INSERT INTO ur1 SELECT concat('r', toString(number)), number FROM numbers(5);
INSERT INTO ur2 SELECT number, number * 10 FROM numbers(3);

SELECT '-- the side is a constant';
SELECT ul.heavy FROM ul, (SELECT 1 FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t ORDER BY ul.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT ul.heavy FROM ul, (SELECT 1 FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t ORDER BY ul.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT ul.heavy FROM ul, (SELECT 1 FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t ORDER BY ul.k DESC LIMIT 5);

SELECT '-- the side is computed from two tables, and nothing of it is needed';
SELECT ul.heavy FROM ul, (SELECT ur1.rheavy || toString(ur2.x) AS s FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t ORDER BY ul.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT ul.heavy FROM ul, (SELECT ur1.rheavy || toString(ur2.x) AS s FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t ORDER BY ul.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT ul.heavy FROM ul, (SELECT ur1.rheavy || toString(ur2.x) AS s FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t
    ORDER BY ul.k DESC LIMIT 5);

SELECT '-- the side is computed from two tables, and it is in the result';
SELECT ul.heavy, t.s FROM ul, (SELECT ur1.rheavy || toString(ur2.x) AS s FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t ORDER BY ul.k DESC, t.s LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT ul.heavy, t.s FROM ul, (SELECT ur1.rheavy || toString(ur2.x) AS s FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t ORDER BY ul.k DESC, t.s LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT ul.heavy, t.s FROM ul, (SELECT ur1.rheavy || toString(ur2.x) AS s FROM ur1 JOIN ur2 ON ur1.k = ur2.k) AS t
    ORDER BY ul.k DESC, t.s LIMIT 5);

DROP TABLE ul;
DROP TABLE ur1;
DROP TABLE ur2;
