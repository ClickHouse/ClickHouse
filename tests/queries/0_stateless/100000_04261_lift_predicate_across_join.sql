-- Tests for query_plan_lift_predicate_across_join

DROP TABLE IF EXISTS lift_orders;
DROP TABLE IF EXISTS lift_lineitem;

CREATE TABLE lift_orders (o_orderkey UInt64, o_data String) ENGINE = MergeTree ORDER BY o_orderkey;
CREATE TABLE lift_lineitem (l_orderkey UInt64, l_data String) ENGINE = MergeTree ORDER BY l_orderkey;

INSERT INTO lift_orders SELECT number, toString(number) FROM numbers(1000);
INSERT INTO lift_lineitem SELECT number, toString(number) FROM numbers(1000);

-- INNER: predicate on left subquery lifts to right
SELECT '== inner ==';
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 0;
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 1;
SELECT 'inner filter steps off', countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Runtime%')
FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
    INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
    SETTINGS query_plan_lift_predicate_across_join = 0
);
SELECT 'inner filter steps on', countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Runtime%')
FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
    INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
    SETTINGS query_plan_lift_predicate_across_join = 1
);

-- LEFT JOIN with predicate on preserved (left) side lifts to right
SELECT '== left preserved ==';
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
LEFT JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 0;
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
LEFT JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 1;
SELECT 'left preserved filter steps on', countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Runtime%')
FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
    LEFT JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
    SETTINGS query_plan_lift_predicate_across_join = 1
);

-- LEFT JOIN with predicate on null-producing (right) side must not lift -
-- it would over-restrict left and drop rows that should appear with NULL right
SELECT '== left null-producing ==';
SELECT count() FROM lift_orders AS o
LEFT JOIN (SELECT * FROM lift_lineitem WHERE l_orderkey = 42) AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 0;
SELECT count() FROM lift_orders AS o
LEFT JOIN (SELECT * FROM lift_lineitem WHERE l_orderkey = 42) AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 1;
SELECT 'left null-producing filter steps on', countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Runtime%')
FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM lift_orders AS o
    LEFT JOIN (SELECT * FROM lift_lineitem WHERE l_orderkey = 42) AS l ON o.o_orderkey = l.l_orderkey
    SETTINGS query_plan_lift_predicate_across_join = 1
);

-- FULL JOIN - no safe direction
SELECT '== full ==';
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
FULL JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 0;
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
FULL JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 1;
SELECT 'full filter steps on', countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Runtime%')
FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42) AS o
    FULL JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
    SETTINGS query_plan_lift_predicate_across_join = 1
);

-- Multiple conjuncts on the equi-key column all propagate
SELECT '== multi conjunct ==';
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey > 40 AND o_orderkey < 50) AS o
INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 0;
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey > 40 AND o_orderkey < 50) AS o
INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 1;

-- Mixed: equi-eligible conjunct lifts, non-equi one stays put
SELECT '== mixed ==';
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42 AND o_data = '42') AS o
INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 0;
SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42 AND o_data = '42') AS o
INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
SETTINGS query_plan_lift_predicate_across_join = 1;
SELECT 'mixed filter steps on', countIf(explain LIKE '%Filter%' AND explain NOT LIKE '%Runtime%')
FROM (
    EXPLAIN PLAN keep_logical_steps = 1
    SELECT count() FROM (SELECT * FROM lift_orders WHERE o_orderkey = 42 AND o_data = '42') AS o
    INNER JOIN lift_lineitem AS l ON o.o_orderkey = l.l_orderkey
    SETTINGS query_plan_lift_predicate_across_join = 1
);

DROP TABLE lift_orders;
DROP TABLE lift_lineitem;
