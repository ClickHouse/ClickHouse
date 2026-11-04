-- A materialized view reads the inserted block wherever its query names the source table,
-- except inside an ordinary view or the `view` table function, which read the table itself.
-- Row 1000 is in the table before the views exist and is never in an inserted block.

-- `implicit_transaction` does not support async inserts, and the results are read right after each insert.
SET async_insert = 0;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP VIEW IF EXISTS v;

CREATE TABLE src (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE dst (mv String, n UInt64) ENGINE = MergeTree ORDER BY (mv, n);
INSERT INTO src VALUES (1000);
CREATE VIEW v AS SELECT n FROM src;

CREATE MATERIALIZED VIEW mv_plain TO dst AS SELECT 'plain' AS mv, n FROM src;
CREATE MATERIALIZED VIEW mv_in TO dst AS SELECT 'in' AS mv, n FROM src WHERE n IN (SELECT max(n) FROM src);
CREATE MATERIALIZED VIEW mv_scalar TO dst AS SELECT 'scalar' AS mv, (SELECT count() FROM src) AS n FROM src LIMIT 1;
CREATE MATERIALIZED VIEW mv_cte TO dst AS WITH t AS (SELECT n FROM src) SELECT 'cte' AS mv, n FROM t;
CREATE MATERIALIZED VIEW mv_join TO dst AS SELECT 'join' AS mv, a.n AS n FROM src AS a INNER JOIN (SELECT n FROM src) AS b ON a.n = b.n;
CREATE MATERIALIZED VIEW mv_view TO dst AS SELECT 'view' AS mv, (SELECT count() FROM v WHERE n >= 1000) AS n FROM src LIMIT 1;
CREATE MATERIALIZED VIEW mv_view_function TO dst AS SELECT 'view_function' AS mv, (SELECT count() FROM view(SELECT n FROM src) WHERE n >= 1000) AS n FROM src LIMIT 1;

SELECT 'without transaction';
INSERT INTO src VALUES (1), (2), (3);
SELECT mv, groupArray(n) FROM (SELECT mv, n FROM dst ORDER BY mv, n) GROUP BY mv ORDER BY mv;

SELECT 'implicit transaction';
-- Ordinary views and the `view` table function do not support transactions.
DROP VIEW mv_view;
DROP VIEW mv_view_function;
TRUNCATE TABLE dst;
INSERT INTO src SETTINGS implicit_transaction = 1 VALUES (4), (5);
SELECT mv, groupArray(n) FROM (SELECT mv, n FROM dst ORDER BY mv, n) GROUP BY mv ORDER BY mv;

DROP VIEW mv_plain;
DROP VIEW mv_in;
DROP VIEW mv_scalar;
DROP VIEW mv_cte;
DROP VIEW mv_join;
DROP VIEW v;
DROP TABLE dst;
DROP TABLE src;
