-- A subquery's outer join running with `join_use_nulls = 0` pads with type defaults even when the query
-- runs with `join_use_nulls = 1`. The conflict detector must not count the `ON` condition of the outer
-- query as rejecting NULLs on those default-padded columns, which would allow regrouping the joins as if
-- they were padded with NULL. Here the padded `0` matches `x3.j = 0` in the query as written.
-- Every query must return the same rows as without reordering.
-- https://github.com/ClickHouse/ClickHouse/issues/127418

SET enable_analyzer = 1;
SET join_use_nulls = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS x1;
DROP TABLE IF EXISTS x2;
DROP TABLE IF EXISTS x3;

CREATE TABLE x1 (k UInt8) ENGINE = MergeTree ORDER BY k;
CREATE TABLE x2 (k UInt8, j UInt8) ENGINE = MergeTree ORDER BY k;
CREATE TABLE x3 (j UInt8, s String) ENGINE = MergeTree ORDER BY j;
INSERT INTO x1 VALUES (1), (2);
INSERT INTO x2 VALUES (1, 10), (3, 30);
INSERT INTO x3 VALUES (0, 'zero'), (10, 'ten');

-- The padded `j = 0` of `x1.k = 2` matches `x3.j = 0`.
SELECT 'LEFT', sub.k, sub.j, x3.s
FROM (SELECT x1.k AS k, x2.j AS j FROM x1 LEFT JOIN x2 ON x1.k = x2.k SETTINGS join_use_nulls = 0) AS sub
LEFT JOIN x3 ON sub.j = x3.j
ORDER BY sub.k
SETTINGS param__internal_join_table_stat_hints = '{"x1": {"cardinality": 100000}, "x2": {"cardinality": 2}, "x3": {"cardinality": 2}}';

-- The padded `k1 = 0` of `x2.k = 3` matches `x3.j = 0`.
SELECT 'FULL', sub.k1, sub.k2, x3.s
FROM (SELECT x1.k AS k1, x2.k AS k2 FROM x1 FULL JOIN x2 ON x1.k = x2.k SETTINGS join_use_nulls = 0) AS sub
LEFT JOIN x3 ON sub.k1 = x3.j
ORDER BY sub.k1
SETTINGS param__internal_join_table_stat_hints = '{"x1": {"cardinality": 2}, "x2": {"cardinality": 100000}, "x3": {"cardinality": 2}}';

-- With the same conditions under the CD-A detector.
SELECT 'LEFT, CD-A', sub.k, sub.j, x3.s
FROM (SELECT x1.k AS k, x2.j AS j FROM x1 LEFT JOIN x2 ON x1.k = x2.k SETTINGS join_use_nulls = 0) AS sub
LEFT JOIN x3 ON sub.j = x3.j
ORDER BY sub.k
SETTINGS query_plan_optimize_join_order_conflict_detector = 'a',
    param__internal_join_table_stat_hints = '{"x1": {"cardinality": 100000}, "x2": {"cardinality": 2}, "x3": {"cardinality": 2}}';

DROP TABLE x1;
DROP TABLE x2;
DROP TABLE x3;
