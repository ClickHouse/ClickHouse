-- An equality unsafe as a join key must not be merged into the join graph by `query_plan_merge_filters_into_join`
-- also when it is computed below another join than the one the filter sits above: that join is flattened into
-- the same graph, so the merged filter would read the resolved equality.

SET enable_analyzer = 1;
SET query_plan_merge_filters_into_join = 1;
SET query_plan_filter_push_down = 0;

DROP TABLE IF EXISTS va;
DROP TABLE IF EXISTS vb;
DROP TABLE IF EXISTS da;
DROP TABLE IF EXISTS db;
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS fa;
DROP TABLE IF EXISTS fb;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS d;

CREATE TABLE c (id UInt64) ENGINE = Memory;
CREATE TABLE d (id UInt64) ENGINE = Memory;
INSERT INTO c VALUES (1), (2);
INSERT INTO d VALUES (1), (2);

SELECT 'Variant';
CREATE TABLE va (id UInt64, v Variant(UInt64, String)) ENGINE = Memory;
CREATE TABLE vb (id UInt64, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO va VALUES (1, NULL), (2, 7);
INSERT INTO vb VALUES (9, NULL), (2, 7);
SELECT q.aid
FROM
(
    SELECT s.aid
    FROM (SELECT va.id AS aid, (va.v = vb.v OR va.id = vb.id) AS cond FROM va CROSS JOIN vb) s
    INNER JOIN c ON s.aid = c.id
    WHERE cond
) q
INNER JOIN d ON q.aid = d.id
ORDER BY ALL;

SELECT 'Dynamic';
CREATE TABLE da (id UInt64, v Dynamic) ENGINE = Memory;
CREATE TABLE db (id UInt64, v Dynamic) ENGINE = Memory;
INSERT INTO da VALUES (1, 7), (2, 8);
INSERT INTO db VALUES (1, 7), (2, 9);
SELECT q.aid
FROM
(
    SELECT s.aid
    FROM (SELECT da.id AS aid, da.v = db.v AS cond FROM da CROSS JOIN db) s
    INNER JOIN c ON s.aid = c.id
    WHERE cond
) q
INNER JOIN d ON q.aid = d.id
ORDER BY ALL;

SELECT 'No common type';
CREATE TABLE ta (id UInt64, v DateTime) ENGINE = Memory;
CREATE TABLE tb (id UInt64, v UInt32) ENGINE = Memory;
INSERT INTO ta VALUES (1, 7), (2, 8);
INSERT INTO tb VALUES (1, 7), (2, 9);
SELECT q.aid
FROM
(
    SELECT s.aid
    FROM (SELECT ta.id AS aid, ta.v = tb.v AS cond FROM ta CROSS JOIN tb) s
    INNER JOIN c ON s.aid = c.id
    WHERE cond
) q
INNER JOIN d ON q.aid = d.id
ORDER BY ALL;

SELECT 'isNotDistinctFrom NaN';
CREATE TABLE fa (id UInt64, v Float64) ENGINE = Memory;
CREATE TABLE fb (id UInt64, v Float64) ENGINE = Memory;
INSERT INTO fa VALUES (1, nan), (2, 7);
INSERT INTO fb VALUES (9, nan), (2, 7);
SELECT q.aid
FROM
(
    SELECT s.aid
    FROM (SELECT fa.id AS aid, (isNotDistinctFrom(fa.v, fb.v) OR fa.id = fb.id) AS cond FROM fa CROSS JOIN fb) s
    INNER JOIN c ON s.aid = c.id
    WHERE cond
) q
INNER JOIN d ON q.aid = d.id
ORDER BY ALL;

-- A filter without such an equality below it is still merged: the three tables are reordered as one graph.
SELECT 'Safe filter is merged';
SELECT count() = 0 FROM
(
    EXPLAIN
    SELECT q.aid
    FROM
    (
        SELECT s.aid
        FROM (SELECT va.id AS aid, va.id = vb.id AS cond FROM va CROSS JOIN vb) s
        INNER JOIN c ON s.aid = c.id
        WHERE cond
    ) q
    INNER JOIN d ON q.aid = d.id
) WHERE explain LIKE '%Filter%';

DROP TABLE va;
DROP TABLE vb;
DROP TABLE da;
DROP TABLE db;
DROP TABLE ta;
DROP TABLE tb;
DROP TABLE fa;
DROP TABLE fb;
DROP TABLE c;
DROP TABLE d;
