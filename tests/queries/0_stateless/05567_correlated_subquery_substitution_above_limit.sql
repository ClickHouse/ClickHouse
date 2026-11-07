-- Equivalent-expression substitution in a correlated subquery must not use an equality that sits
-- above a `LIMIT` / `OFFSET` of the correlated part: such an equality does not constrain the rows
-- the `LIMIT` / `OFFSET` chooses from within one evaluation of the subquery.

SET allow_correlated_subqueries = 1;
SET correlated_subqueries_substitute_equivalent_expressions = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
-- Pinned because filter-to-Prewhere relocation, filter merging, and the join-order conversion
-- change which step descriptions are visible to the plan probe below.
SET optimize_move_to_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_merge_filter_into_join_condition = 0;

CREATE TABLE t (id Int32, owner Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 10), (2, 10), (3, 30);

SELECT '-- case 1: EXISTS, derived table with ORDER BY LIMIT, equality above the LIMIT';
SELECT r.id FROM t AS r
WHERE EXISTS (SELECT 1 FROM (SELECT p.id AS u FROM t AS p WHERE p.owner = r.owner ORDER BY p.id LIMIT 1) AS s WHERE s.u = r.id)
ORDER BY r.id;

SELECT '-- case 2: OFFSET without LIMIT';
SELECT r.id FROM t AS r
WHERE EXISTS (SELECT 1 FROM (SELECT p.id AS u FROM t AS p WHERE p.owner = r.owner ORDER BY p.id OFFSET 1) AS s WHERE s.u = r.id)
ORDER BY r.id;

SELECT '-- case 3: LIMIT with OFFSET';
SELECT r.id FROM t AS r
WHERE EXISTS (SELECT 1 FROM (SELECT p.id AS u FROM t AS p WHERE p.owner = r.owner ORDER BY p.id LIMIT 1 OFFSET 1) AS s WHERE s.u = r.id)
ORDER BY r.id;

SELECT '-- case 4: scalar subquery';
SELECT r.id, ifNull((SELECT count() FROM (SELECT p.id AS u FROM t AS p WHERE p.owner = r.owner ORDER BY p.id LIMIT 1) AS s WHERE s.u = r.id), 0)
FROM t AS r
ORDER BY r.id;

SELECT '-- case 5: GROUP BY under the LIMIT';
SELECT r.id FROM t AS r
WHERE EXISTS (SELECT 1 FROM (SELECT p.id AS u, count() FROM t AS p WHERE p.owner = r.owner GROUP BY p.id ORDER BY p.id LIMIT 1) AS s WHERE s.u = r.id)
ORDER BY r.id;

SELECT '-- case 6: case 1 with the left decorrelation join kind';
SELECT r.id FROM t AS r
WHERE EXISTS (SELECT 1 FROM (SELECT p.id AS u FROM t AS p WHERE p.owner = r.owner ORDER BY p.id LIMIT 1) AS s WHERE s.u = r.id)
ORDER BY r.id
SETTINGS correlated_subqueries_default_join_kind = 'left';

SELECT '-- case 7: JOIN LATERAL';
SET enable_lateral_join = 1;
SELECT r.id, x.u FROM t AS r
INNER JOIN LATERAL (SELECT s.u FROM (SELECT p.id AS u FROM t AS p WHERE p.owner = r.owner ORDER BY p.id LIMIT 1) AS s WHERE s.u = r.id) AS x ON true
ORDER BY r.id;

SELECT '-- case 8: equalities below the LIMIT are still substituted';
SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT r.id FROM t AS r WHERE EXISTS (SELECT 1 FROM t AS p WHERE p.owner = r.owner AND p.id = r.id ORDER BY p.id LIMIT 1));
SELECT r.id FROM t AS r
WHERE EXISTS (SELECT 1 FROM t AS p WHERE p.owner = r.owner AND p.id = r.id ORDER BY p.id LIMIT 1)
ORDER BY r.id;

SELECT '-- case 9: equality above the LIMIT on a column determined by an equality below it';
CREATE TABLE t2 (id Int32, owner Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (1, 1), (2, 10), (3, 3);
SELECT r.id FROM t2 AS r
WHERE EXISTS (SELECT 1 FROM (SELECT p.id AS u FROM t2 AS p WHERE p.id = r.owner ORDER BY p.id LIMIT 1) AS s WHERE s.u = r.id)
ORDER BY r.id;

DROP TABLE t;
DROP TABLE t2;
