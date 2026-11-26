-- Tags: no-parallel
-- ^ creates global SQL UDFs; concurrent copies of this test would clash on the names.

DROP VIEW IF EXISTS v_05878;
DROP TABLE IF EXISTS t_05878;
DROP FUNCTION IF EXISTS wrap_except_05878;
DROP FUNCTION IF EXISTS except_05878;
DROP FUNCTION IF EXISTS qualified_05878;
DROP FUNCTION IF EXISTS columns_05878;
DROP FUNCTION IF EXISTS wrap_columns_05878;
DROP FUNCTION IF EXISTS identity_05878;

CREATE FUNCTION except_05878 AS (p0) -> tuple(* EXCEPT (p0));
CREATE FUNCTION wrap_except_05878 AS (p0) -> except_05878(p0);
CREATE FUNCTION qualified_05878 AS (p0) -> tuple(t.COLUMNS(p0, q));
CREATE FUNCTION columns_05878 AS (p0) -> tuple(COLUMNS(p0, q));
CREATE FUNCTION identity_05878 AS (x) -> x;
CREATE FUNCTION wrap_columns_05878 AS (x) -> columns_05878(identity_05878(x));

CREATE TABLE t_05878 (p0 UInt8, q UInt8) ENGINE = Memory;
INSERT INTO t_05878 VALUES (1, 2);

SELECT except_05878(1 + 1) AS e1, except_05878(q) AS e2, wrap_except_05878(1 + 1) AS w,
    qualified_05878(1 + 1) AS q1, qualified_05878(q) AS q2, columns_05878(q) AS c, wrap_columns_05878(q) AS wc
FROM t_05878 AS t;

CREATE VIEW v_05878 AS SELECT except_05878(1 + 1) AS e1, except_05878(q) AS e2, wrap_except_05878(1 + 1) AS w,
    qualified_05878(1 + 1) AS q1, qualified_05878(q) AS q2, columns_05878(q) AS c, wrap_columns_05878(q) AS wc
FROM t_05878 AS t;

SELECT columns_05878(1 + 1) FROM t_05878; -- { serverError UNKNOWN_IDENTIFIER }
CREATE VIEW v_columns_05878 AS SELECT columns_05878(1 + 1) FROM t_05878; -- { serverError UNKNOWN_IDENTIFIER }
SELECT wrap_columns_05878(1 + 1) FROM t_05878; -- { serverError UNKNOWN_IDENTIFIER }
CREATE VIEW v_wrap_columns_05878 AS SELECT wrap_columns_05878(1 + 1) FROM t_05878; -- { serverError UNKNOWN_IDENTIFIER }
CREATE TABLE t_default_05878 (p0 UInt8, c Tuple(UInt8) MATERIALIZED except_05878(1 + 1)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

DROP FUNCTION wrap_columns_05878;
DROP FUNCTION identity_05878;
DROP FUNCTION wrap_except_05878;
DROP FUNCTION except_05878;
DROP FUNCTION qualified_05878;
DROP FUNCTION columns_05878;
SELECT * FROM v_05878;

DROP VIEW v_05878;
DROP TABLE t_05878;
