-- The `array_join_use_nulls` mismatch guard of a view follows the effective value of the setting
-- in the scope of each `LEFT ARRAY JOIN`, exactly as the query is executed.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS v_compound;
DROP VIEW IF EXISTS v_last_wins_off;
DROP VIEW IF EXISTS v_last_wins_on;
DROP VIEW IF EXISTS v_lc_nonempty;
DROP VIEW IF EXISTS v_lc_empty;

SET array_join_use_nulls = 0;

-- The trailing `SETTINGS` clause of `A UNION ALL B INTERSECT ALL C` sits on the last operand of the nested
-- `INTERSECT` and applies to all operands, including `A` with the `LEFT ARRAY JOIN`.
CREATE VIEW v_compound (x UInt8) SQL SECURITY INVOKER AS
SELECT x FROM
(
    SELECT x
    FROM (SELECT CAST([1], 'Array(Nullable(UInt8))') AS arr)
    LEFT ARRAY JOIN arr AS x
    UNION ALL
    SELECT toNullable(toUInt8(2)) AS x
    INTERSECT ALL
    SELECT toNullable(toUInt8(2)) AS x
    SETTINGS array_join_use_nulls = 0
);

SELECT 'compound trailing settings';
SELECT x, toTypeName(x) FROM v_compound ORDER BY x SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 0;
SELECT x, toTypeName(x) FROM v_compound ORDER BY x SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 0;
SELECT x, toTypeName(x) FROM v_compound ORDER BY x SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 1;

-- A setting assigned several times in one clause: the last assignment wins.
CREATE VIEW v_last_wins_off (x UInt8) AS
SELECT x FROM (SELECT CAST([1], 'Array(Nullable(UInt8))') AS arr) LEFT ARRAY JOIN arr AS x
SETTINGS array_join_use_nulls = 1, array_join_use_nulls = 0;

CREATE VIEW v_last_wins_on (x UInt8) AS
SELECT x FROM (SELECT [1] AS arr) LEFT ARRAY JOIN arr AS x
SETTINGS array_join_use_nulls = 0, array_join_use_nulls = 1;

SELECT 'last assignment wins';
SELECT x, toTypeName(x) FROM v_last_wins_off SETTINGS array_join_use_nulls = 0;
SELECT x, toTypeName(x) FROM v_last_wins_off SETTINGS array_join_use_nulls = 1;
SELECT x, toTypeName(x) FROM v_last_wins_on SETTINGS array_join_use_nulls = 0; -- { serverError INCORRECT_QUERY }

-- `LowCardinality(String)` becomes `LowCardinality(Nullable(String))`: the mismatch is rejected with and without inlining.
CREATE VIEW v_lc_nonempty SQL SECURITY INVOKER AS
SELECT x FROM (SELECT CAST(['a'], 'Array(LowCardinality(String))') AS arr) LEFT ARRAY JOIN arr AS x;
CREATE VIEW v_lc_empty SQL SECURITY INVOKER AS
SELECT x FROM (SELECT CAST([], 'Array(LowCardinality(String))') AS arr) LEFT ARRAY JOIN arr AS x;

SELECT 'LowCardinality';
SELECT x, toTypeName(x) FROM v_lc_nonempty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 0; -- { serverError INCORRECT_QUERY }
SELECT x, toTypeName(x) FROM v_lc_nonempty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 1; -- { serverError INCORRECT_QUERY }
SELECT x, toTypeName(x) FROM v_lc_empty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 0; -- { serverError INCORRECT_QUERY }
SELECT x, toTypeName(x) FROM v_lc_empty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 1; -- { serverError INCORRECT_QUERY }
SELECT x, toTypeName(x) FROM v_lc_nonempty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 0;
SELECT x, toTypeName(x) FROM v_lc_nonempty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 1;
SELECT x, toTypeName(x) FROM v_lc_empty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 0;
SELECT x, toTypeName(x) FROM v_lc_empty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 1;

DROP VIEW v_compound;
DROP VIEW v_last_wins_off;
DROP VIEW v_last_wins_on;
DROP VIEW v_lc_nonempty;
DROP VIEW v_lc_empty;
