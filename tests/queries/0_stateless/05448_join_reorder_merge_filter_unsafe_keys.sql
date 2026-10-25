-- A `WHERE` condition merged into the join graph by `query_plan_merge_filters_into_join` must not turn
-- into a join key that the join cannot use as written: an equality over `Variant` (where `NULL` matches
-- `NULL` as a key) or over `Dynamic`, or an extra key for a prepared `Join` engine table.

SET enable_analyzer = 1;
SET query_plan_merge_filters_into_join = 1;

DROP TABLE IF EXISTS va;
DROP TABLE IF EXISTS vb;
DROP TABLE IF EXISTS da;
DROP TABLE IF EXISTS db;
DROP TABLE IF EXISTS ja;
DROP TABLE IF EXISTS jb;
DROP TABLE IF EXISTS c;

CREATE TABLE c (id UInt64) ENGINE = Memory;
INSERT INTO c VALUES (1), (2);

SELECT 'Variant';
CREATE TABLE va (id UInt64, v Variant(UInt64, String)) ENGINE = Memory;
CREATE TABLE vb (id UInt64, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO va VALUES (1, NULL), (2, 7);
INSERT INTO vb VALUES (9, NULL), (2, 7);

SELECT s.aid
FROM (SELECT va.id AS aid FROM va CROSS JOIN vb WHERE va.v = vb.v OR va.id = vb.id) AS s
INNER JOIN c ON s.aid = c.id
ORDER BY ALL;

SELECT s.aid
FROM (SELECT va.id AS aid FROM va INNER JOIN vb ON va.id = vb.id WHERE va.v = vb.v) AS s
INNER JOIN c ON s.aid = c.id
ORDER BY ALL;

SELECT 'Dynamic';
CREATE TABLE da (id UInt64, v Dynamic) ENGINE = Memory;
CREATE TABLE db (id UInt64, v Dynamic) ENGINE = Memory;
INSERT INTO da VALUES (1, 7);
INSERT INTO db VALUES (1, 7);

SELECT s.aid
FROM (SELECT da.id AS aid FROM da INNER JOIN db ON da.id = db.id WHERE da.v = db.v) AS s
INNER JOIN c ON s.aid = c.id
ORDER BY ALL;

SELECT 'Join engine';
CREATE TABLE ja (id UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE jb (id UInt64, v UInt64) ENGINE = Join(ALL, INNER, id);
INSERT INTO ja VALUES (1, 7), (2, 8);
INSERT INTO jb VALUES (1, 7), (2, 9);

SELECT s.aid
FROM (SELECT ja.id AS aid FROM ja INNER JOIN jb ON ja.id = jb.id WHERE ja.v = jb.v) AS s
INNER JOIN c ON s.aid = c.id
ORDER BY ALL;

DROP TABLE va;
DROP TABLE vb;
DROP TABLE da;
DROP TABLE db;
DROP TABLE ja;
DROP TABLE jb;
DROP TABLE c;
