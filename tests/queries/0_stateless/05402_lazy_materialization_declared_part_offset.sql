-- Lazy materialization addresses a row by the virtual columns `_part_starting_offset` and `_part_offset`. A column of
-- the table with one of these names takes their place, so such a table is not read lazily, with a join or without one.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS po;
DROP TABLE IF EXISTS pso;
DROP TABLE IF EXISTS r;

CREATE TABLE po (k UInt64, _part_offset UInt64, s UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE pso (k UInt64, _part_starting_offset UInt64, s UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k;

INSERT INTO po SELECT number, 0, 1000 - number, concat('p', toString(number)) FROM numbers(1000);
INSERT INTO pso SELECT number, 5, 1000 - number, concat('p', toString(number)) FROM numbers(1000);
INSERT INTO r SELECT number, concat('r', toString(number)) FROM numbers(1000);

SELECT '-- without a join';
SELECT k, payload FROM po ORDER BY s LIMIT 3;
SELECT k, payload FROM pso ORDER BY s LIMIT 3;

SELECT '-- with a join';
SELECT po.k, po.payload, r.rheavy FROM po JOIN r ON po.k = r.k ORDER BY po.s LIMIT 3;
SELECT pso.k, pso.payload, r.rheavy FROM pso JOIN r ON pso.k = r.k ORDER BY pso.s LIMIT 3;

SELECT '-- the other side of the join is still read lazily';
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT po.k, po.payload, r.rheavy FROM po JOIN r ON po.k = r.k ORDER BY po.s LIMIT 3);

DROP TABLE po;
DROP TABLE pso;
DROP TABLE r;
