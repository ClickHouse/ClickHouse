-- The conjuncts of a `WHERE` condition keep their written order when the condition is split into atoms:
-- a conjunct can guard the ones written after it, so a throwing conversion must not move before its guard.

SET enable_analyzer = 1;
SET short_circuit_function_evaluation_reorder_arguments = 0;

DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;

CREATE TABLE a (id UInt64, s String) ENGINE = Memory;
CREATE TABLE b (id UInt64, s String) ENGINE = Memory;
CREATE TABLE c (id UInt64) ENGINE = Memory;
INSERT INTO a VALUES (1, ''), (2, '1');
INSERT INTO b VALUES (1, ''), (2, '1');
INSERT INTO c VALUES (1), (2);

-- `ActionsDAG::extractConjunctionAtoms` visits a shared atom once and has to keep its first occurrence.
SELECT 'Repeated guard, filter push down';
SELECT a.id FROM a INNER JOIN b ON a.id = b.id
WHERE notEmpty(a.s) AND toInt32(a.s) > 0 AND notEmpty(a.s)
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 0;

SELECT a.id FROM a INNER JOIN b ON a.id = b.id
WHERE notEmpty(a.s) AND toInt32(a.s) > 0 AND notEmpty(a.s)
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1;

-- A filter merged into the join graph becomes one edge per conjunct, in the written order.
SELECT 'Two conjuncts merged into the join graph';
SELECT q.aid
FROM
(
    SELECT a.id AS aid
    FROM a INNER JOIN b ON a.id = b.id
    WHERE notEmpty(concat(a.s, b.s)) AND toInt32(concat(a.s, b.s)) > 0
) q
INNER JOIN c ON q.aid = c.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1;

SELECT 'Repeated guard merged into the join graph';
SELECT q.aid
FROM
(
    SELECT a.id AS aid
    FROM a INNER JOIN b ON a.id = b.id
    WHERE notEmpty(concat(a.s, b.s)) AND toInt32(concat(a.s, b.s)) > 0 AND notEmpty(concat(a.s, b.s))
) q
INNER JOIN c ON q.aid = c.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1;

SELECT 'Join order algorithms';
SELECT q.aid
FROM
(
    SELECT a.id AS aid
    FROM a INNER JOIN b ON a.id = b.id
    WHERE notEmpty(concat(a.s, b.s)) AND toInt32(concat(a.s, b.s)) > 0
) q
INNER JOIN c ON q.aid = c.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, query_plan_optimize_join_order_algorithm = 'greedy';

SELECT q.aid
FROM
(
    SELECT a.id AS aid
    FROM a INNER JOIN b ON a.id = b.id
    WHERE notEmpty(concat(a.s, b.s)) AND toInt32(concat(a.s, b.s)) > 0
) q
INNER JOIN c ON q.aid = c.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, query_plan_optimize_join_order_algorithm = 'dpsize,greedy';

SELECT q.aid
FROM
(
    SELECT a.id AS aid
    FROM a INNER JOIN b ON a.id = b.id
    WHERE notEmpty(concat(a.s, b.s)) AND toInt32(concat(a.s, b.s)) > 0
) q
INNER JOIN c ON q.aid = c.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, query_plan_optimize_join_order_algorithm = 'dpsub,greedy';

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
