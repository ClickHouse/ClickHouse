-- The `array_join_use_nulls` mismatch between a view's stored structure and the query is rejected
-- whether or not `analyzer_inline_views` inlines the view.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS v_nonempty;
DROP VIEW IF EXISTS v_empty;

SET array_join_use_nulls = 0;
CREATE VIEW v_nonempty SQL SECURITY INVOKER AS SELECT x FROM (SELECT [1] AS arr) LEFT ARRAY JOIN arr AS x;
CREATE VIEW v_empty SQL SECURITY INVOKER AS SELECT x FROM (SELECT CAST([], 'Array(UInt8)') AS arr) LEFT ARRAY JOIN arr AS x;

SELECT x, toTypeName(x) FROM v_nonempty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 0; -- { serverError INCORRECT_QUERY }
SELECT x, toTypeName(x) FROM v_nonempty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 1; -- { serverError INCORRECT_QUERY }
SELECT x, toTypeName(x) FROM v_empty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 0; -- { serverError INCORRECT_QUERY }
SELECT x, toTypeName(x) FROM v_empty SETTINGS array_join_use_nulls = 1, analyzer_inline_views = 1; -- { serverError INCORRECT_QUERY }

-- Without the mismatch, both paths return the same result.
SELECT x, toTypeName(x) FROM v_nonempty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 0;
SELECT x, toTypeName(x) FROM v_nonempty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 1;
SELECT x, toTypeName(x) FROM v_empty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 0;
SELECT x, toTypeName(x) FROM v_empty SETTINGS array_join_use_nulls = 0, analyzer_inline_views = 1;

DROP VIEW v_nonempty;
DROP VIEW v_empty;
