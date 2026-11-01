-- Merging `WHERE` conditions into the join graph by `query_plan_merge_filters_into_join`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;

CREATE TABLE a (id UInt64, v DateTime, x Int64) ENGINE = Memory;
CREATE TABLE b (id UInt64, v UInt32, x Int64) ENGINE = Memory;
CREATE TABLE c (id UInt64) ENGINE = Memory;
INSERT INTO a VALUES (1, 7, 1);
INSERT INTO b VALUES (1, 7, 1);
INSERT INTO c VALUES (1);

-- An equality whose operands have no common supertype cannot become a join key, so the filter must stay a filter.
SELECT s.aid
FROM
(
    SELECT a.id AS aid
    FROM a INNER JOIN b ON a.id = b.id
    WHERE a.v = b.v
) AS s
INNER JOIN c ON s.aid = c.id
SETTINGS query_plan_merge_filters_into_join = 0;

SELECT s.aid
FROM
(
    SELECT a.id AS aid
    FROM a INNER JOIN b ON a.id = b.id
    WHERE a.v = b.v
) AS s
INNER JOIN c ON s.aid = c.id
SETTINGS query_plan_merge_filters_into_join = 1;

-- A condition with shared conjunction nodes must be flattened into a linear number of join-graph edges.
SELECT count() FROM (SELECT id FROM (SELECT id, p23 AND p23 AS p24 FROM (SELECT id, p22 AND p22 AS p23 FROM (SELECT id, p21 AND p21 AS p22 FROM (SELECT id, p20 AND p20 AS p21 FROM (SELECT id, p19 AND p19 AS p20 FROM (SELECT id, p18 AND p18 AS p19 FROM (SELECT id, p17 AND p17 AS p18 FROM (SELECT id, p16 AND p16 AS p17 FROM (SELECT id, p15 AND p15 AS p16 FROM (SELECT id, p14 AND p14 AS p15 FROM (SELECT id, p13 AND p13 AS p14 FROM (SELECT id, p12 AND p12 AS p13 FROM (SELECT id, p11 AND p11 AS p12 FROM (SELECT id, p10 AND p10 AS p11 FROM (SELECT id, p9 AND p9 AS p10 FROM (SELECT id, p8 AND p8 AS p9 FROM (SELECT id, p7 AND p7 AS p8 FROM (SELECT id, p6 AND p6 AS p7 FROM (SELECT id, p5 AND p5 AS p6 FROM (SELECT id, p4 AND p4 AS p5 FROM (SELECT id, p3 AND p3 AS p4 FROM (SELECT id, p2 AND p2 AS p3 FROM (SELECT id, p1 AND p1 AS p2 FROM (SELECT id, p0 AND p0 AS p1 FROM (SELECT a.id AS id, a.x + b.x > 0 AS p0 FROM a INNER JOIN b ON a.id = b.id))))))))))))))))))))))))) WHERE p24) AS s INNER JOIN c ON s.id = c.id
SETTINGS query_plan_merge_filters_into_join = 1, query_plan_filter_push_down = 0, compile_expressions = 0;

DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
