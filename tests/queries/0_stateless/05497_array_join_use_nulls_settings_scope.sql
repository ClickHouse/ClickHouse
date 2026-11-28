-- The `LEFT ARRAY JOIN` mismatch error of views and materialized views is raised only when `array_join_use_nulls`
-- is in effect in the scope of the query owning the `LEFT ARRAY JOIN`, so a `Nullable` element type keeps being
-- converted to the declared type otherwise.

-- A `SETTINGS` clause of a subquery without `LEFT ARRAY JOIN` does not affect the outer query.
DROP VIEW IF EXISTS v_aj_scope;
CREATE VIEW v_aj_scope (x UInt8) AS
SELECT x
FROM
(
    SELECT CAST([1], 'Array(Nullable(UInt8))') AS arr
    SETTINGS array_join_use_nulls = 1
)
LEFT ARRAY JOIN arr AS x
SETTINGS array_join_use_nulls = 0;

SELECT 'nested scope';
SELECT x, toTypeName(x) FROM v_aj_scope SETTINGS array_join_use_nulls = 0;
SELECT x, toTypeName(x) FROM v_aj_scope SETTINGS array_join_use_nulls = 1;
DROP VIEW v_aj_scope;

-- `SETTINGS array_join_use_nulls = DEFAULT` resets the setting to its default value, which is disabled.
CREATE VIEW v_aj_scope (x UInt8) AS
SELECT x FROM (SELECT CAST([1], 'Array(Nullable(UInt8))') AS arr) LEFT ARRAY JOIN arr AS x
SETTINGS array_join_use_nulls = DEFAULT;

SELECT 'reset';
SELECT x, toTypeName(x) FROM v_aj_scope SETTINGS array_join_use_nulls = 1;
DROP VIEW v_aj_scope;

-- The setting enabled in the scope of the `LEFT ARRAY JOIN` is still reported.
CREATE VIEW v_aj_scope (x UInt8) AS
SELECT x
FROM
(
    SELECT x FROM (SELECT [1] AS arr) LEFT ARRAY JOIN arr AS x
    SETTINGS array_join_use_nulls = 1
)
SETTINGS array_join_use_nulls = 0;

SELECT 'enabled in subquery';
SELECT x FROM v_aj_scope SETTINGS array_join_use_nulls = 0; -- { serverError INCORRECT_QUERY }
DROP VIEW v_aj_scope;

-- The same applies to materialized views.
DROP TABLE IF EXISTS t_aj_scope_src;
DROP TABLE IF EXISTS t_aj_scope_dst;
DROP VIEW IF EXISTS mv_aj_scope;
CREATE TABLE t_aj_scope_src (id UInt8) ENGINE = Memory;
CREATE TABLE t_aj_scope_dst (x UInt8) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_aj_scope TO t_aj_scope_dst AS
SELECT x
FROM
(
    SELECT CAST([id], 'Array(Nullable(UInt8))') AS arr FROM t_aj_scope_src
    SETTINGS array_join_use_nulls = 1
)
LEFT ARRAY JOIN arr AS x
SETTINGS array_join_use_nulls = 0;

SELECT 'materialized view';
INSERT INTO t_aj_scope_src SETTINGS array_join_use_nulls = 0 VALUES (1);
INSERT INTO t_aj_scope_src SETTINGS array_join_use_nulls = 1 VALUES (2);
SELECT x, toTypeName(x) FROM t_aj_scope_dst ORDER BY x;

DROP VIEW mv_aj_scope;
DROP TABLE t_aj_scope_dst;
DROP TABLE t_aj_scope_src;
