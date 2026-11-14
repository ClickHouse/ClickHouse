-- A `WHERE` condition merged into the join graph by `query_plan_merge_filters_into_join` is planned by
-- DPsub with a conflict detector, which also flattens the null-supplying side of an outer join. A
-- condition on that side is applied above the outer join, on its padded rows, so the result must be
-- the same as with the setting disabled.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET query_plan_filter_push_down = 0;

DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS tc;
DROP TABLE IF EXISTS td;

CREATE TABLE ta (id UInt64, x UInt64) ENGINE = Memory;
CREATE TABLE tb (id UInt64, x UInt64) ENGINE = Memory;
CREATE TABLE tc (id UInt64, x UInt64) ENGINE = Memory;
CREATE TABLE td (id UInt64) ENGINE = Memory;

INSERT INTO ta VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO tb VALUES (1, 0), (2, 5), (3, 7);
INSERT INTO tc VALUES (1, 100), (2, 200), (4, 400);
INSERT INTO td VALUES (1), (2), (3), (4);

-- The filter reads the null-supplying side `tb`, so it removes the rows of `ta` without a match.
SELECT s.aid, s.bx
FROM (SELECT ta.id AS aid, tb.x AS bx FROM ta LEFT JOIN (SELECT tb.id AS id, tb.x AS x FROM tb JOIN tc ON tb.id = tc.id) AS tb ON ta.id = tb.id WHERE tb.x = 0) AS s
INNER JOIN td ON s.aid = td.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 0;

SELECT s.aid, s.bx
FROM (SELECT ta.id AS aid, tb.x AS bx FROM ta LEFT JOIN (SELECT tb.id AS id, tb.x AS x FROM tb JOIN tc ON tb.id = tc.id) AS tb ON ta.id = tb.id WHERE tb.x = 0) AS s
INNER JOIN td ON s.aid = td.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1;

SELECT '--';

-- The filter reads only the preserved side `ta`.
SELECT s.aid, s.bx
FROM (SELECT ta.id AS aid, tb.x AS bx FROM ta LEFT JOIN tb ON ta.id = tb.id WHERE ta.x > 15) AS s
INNER JOIN td ON s.aid = td.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 0;

SELECT s.aid, s.bx
FROM (SELECT ta.id AS aid, tb.x AS bx FROM ta LEFT JOIN tb ON ta.id = tb.id WHERE ta.x > 15) AS s
INNER JOIN td ON s.aid = td.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1;

SELECT '--';

-- The filter reads both sides of a `FULL` join.
SELECT s.aid, s.bid
FROM (SELECT ta.id AS aid, tb.id AS bid FROM ta FULL JOIN tb ON ta.id = tb.id WHERE ta.x + tb.x < 25) AS s
INNER JOIN td ON s.aid = td.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 0;

SELECT s.aid, s.bid
FROM (SELECT ta.id AS aid, tb.id AS bid FROM ta FULL JOIN tb ON ta.id = tb.id WHERE ta.x + tb.x < 25) AS s
INNER JOIN td ON s.aid = td.id
ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1;

DROP TABLE ta;
DROP TABLE tb;
DROP TABLE tc;
DROP TABLE td;
