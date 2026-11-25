-- Cases of `column_and_query_name_matching = 'standard'` found in review: quote pins and folded name collisions in the analyzer.

SET enable_analyzer = 1;
SET column_and_query_name_matching = 'standard';

SELECT '-- CTE redefinition: the quote of each definition is checked, not only of the first one';
SELECT * FROM (WITH "c" AS (SELECT 1 AS x), c AS (SELECT 2 AS x) SELECT * FROM c) SETTINGS analyzer_compatibility_allow_cte_redefinition = 1;
SELECT * FROM (WITH c AS (SELECT 1 AS x), "c" AS (SELECT 2 AS x) SELECT * FROM c) SETTINGS analyzer_compatibility_allow_cte_redefinition = 1;
SELECT * FROM (WITH c AS (SELECT 1 AS x), "c" AS (SELECT 2 AS x) SELECT * FROM "c") SETTINGS analyzer_compatibility_allow_cte_redefinition = 1;
SELECT * FROM (WITH c AS (SELECT 1 AS x), c AS (SELECT x + 10 AS x FROM c) SELECT * FROM C) SETTINGS analyzer_compatibility_allow_cte_redefinition = 1;

SELECT '-- ARRAY JOIN aliases differing only in case collide, double-quoted ones do not';
SELECT x FROM numbers(1) ARRAY JOIN [1] AS X, [2] AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT x, "X" FROM numbers(1) ARRAY JOIN [1] AS "X", [2] AS x;

SELECT '-- A folded reference to an alias with conflicting definitions is still rejected';
WITH 1 AS Foo, 2 AS Foo SELECT foo; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT '-- INTERPOLATE targets folding to one column';
SELECT k, Val FROM (SELECT 0 AS k, 10 AS Val UNION ALL SELECT 2, 20) ORDER BY k WITH FILL FROM 0 TO 3 INTERPOLATE (Val AS Val + 1, val AS val + 2); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT k, Val FROM (SELECT 0 AS k, 10 AS Val UNION ALL SELECT 2, 20) ORDER BY k WITH FILL FROM 0 TO 3 INTERPOLATE (val AS val + 2);

SELECT '-- NATURAL JOIN over pinned columns';
SELECT * FROM (SELECT 1 AS "Foo") AS l NATURAL JOIN (SELECT 1 AS "Foo") AS r;
SELECT * FROM (SELECT 1 AS "Foo", 2 AS b) AS l NATURAL JOIN (SELECT 1 AS foo, 2 AS B) AS r;
SELECT * FROM (SELECT 1 AS Foo, 2 AS b) AS l NATURAL JOIN (SELECT 1 AS foo, 3 AS c) AS r;

SELECT '-- The merged USING key keeps the pin of its columns';
SELECT foo FROM (SELECT * FROM (SELECT 1 AS "Foo") AS l JOIN (SELECT 1 AS "Foo") AS r USING ("Foo")); -- { serverError UNKNOWN_IDENTIFIER }
SELECT foo FROM (SELECT "Foo" FROM (SELECT 1 AS "Foo") AS l JOIN (SELECT 1 AS "Foo") AS r USING ("Foo")); -- { serverError UNKNOWN_IDENTIFIER }
SELECT foo FROM (SELECT * FROM (SELECT 1 AS "Foo") AS l FULL JOIN (SELECT 1 AS "Foo") AS r USING ("Foo")); -- { serverError UNKNOWN_IDENTIFIER }
SELECT foo FROM (SELECT "Foo" FROM (SELECT 1 AS "Foo") AS l FULL JOIN (SELECT 1 AS "Foo") AS r USING ("Foo")); -- { serverError UNKNOWN_IDENTIFIER }
SELECT "Foo" FROM (SELECT * FROM (SELECT 1 AS "Foo") AS l JOIN (SELECT 1 AS "Foo") AS r USING ("Foo"));
SELECT foo FROM (SELECT * FROM (SELECT 1 AS Foo) AS l JOIN (SELECT 1 AS Foo) AS r USING (foo));

SELECT '-- A lambda argument does not capture a folded name of an alias expression written outside of the lambda';
WITH Col + 1 AS n SELECT arrayMap(cOl -> n, [1]) FROM (SELECT 100 AS Col);
WITH t LIKE '%_1%' AS issue SELECT arrayFilter((T, t2) -> NOT issue, ['a_1', 'b'], [1, 2]);

SELECT '-- A one-part dotted name is a column and its subcolumn';
SELECT `Data.Name`, `data.name`, "Data.Name" FROM (SELECT CAST(tuple(1), 'Tuple(Name UInt8)') AS Data);
SELECT "data.Name" FROM (SELECT CAST(tuple(1), 'Tuple(Name UInt8)') AS Data); -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- Explicit double-quoted projection aliases stay pinned when the expression is replaced during resolution';
SELECT foo FROM (SELECT 1 + 1 AS "Foo"); -- { serverError UNKNOWN_IDENTIFIER }
SELECT foo FROM (SELECT number AS "Foo" FROM numbers(1)); -- { serverError UNKNOWN_IDENTIFIER }
SELECT foo FROM (SELECT [1] AS "Foo"); -- { serverError UNKNOWN_IDENTIFIER }
SELECT "Foo" FROM (SELECT number AS "Foo" FROM numbers(1));

SELECT '-- Materialized CTE keeps the pins of its projection';
WITH c AS MATERIALIZED (SELECT 1 AS "Foo") SELECT foo FROM c SETTINGS enable_materialized_cte = 1; -- { serverError UNKNOWN_IDENTIFIER }
WITH c AS MATERIALIZED (SELECT 1 AS "Foo") SELECT "Foo" FROM c SETTINGS enable_materialized_cte = 1;

SELECT '-- A foldable EXCEPT target does not reach a pinned column';
SELECT * EXCEPT (foo) FROM (SELECT 1 AS "Foo", 2 AS b);
SELECT * EXCEPT ("Foo") FROM (SELECT 1 AS "Foo", 2 AS b);

SELECT '-- The right side of IN keeps the quotes of a table name';
DROP TABLE IF EXISTS Tab_05876;
CREATE TABLE Tab_05876 (x UInt8) ENGINE = Memory;
INSERT INTO Tab_05876 VALUES (1);
SELECT 1 IN tab_05876, 1 IN "Tab_05876" SETTINGS database_and_table_name_matching = 'standard';
SELECT 1 IN "tab_05876" SETTINGS database_and_table_name_matching = 'standard'; -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE Tab_05876;
