-- The [NOT] [I]LIKE, WHERE and LIMIT clauses of SHOW DATABASES / CLUSTERS / CLUSTER /
-- MERGES / [CHANGED] SETTINGS / TABLES / DICTIONARIES, per docs/reference/statements/show.mdx.
-- A SHOW statement typed at the client is parsed by the client, so formatQuerySingleLine()
-- is used to run the same parser on the server and to print the resulting AST back as text.

SELECT '-- accepted forms, printed back from the parsed AST';
SELECT formatQuerySingleLine('SHOW DATABASES ILIKE \'%sys%\' LIMIT 3');
SELECT formatQuerySingleLine('SHOW DATABASES NOT LIKE \'a\'');
SELECT formatQuerySingleLine('SHOW CLUSTERS NOT ILIKE \'%x%\' LIMIT 1 + 1');
SELECT formatQuerySingleLine('SHOW MERGES NOT ILIKE \'a\'');
SELECT formatQuerySingleLine('SHOW CHANGED SETTINGS ILIKE \'%max%\'');
SELECT formatQuerySingleLine('SHOW TABLES FROM db1 ILIKE \'%t%\' LIMIT 2');
SELECT formatQuerySingleLine('SHOW DICTIONARIES IN db1 WHERE name != \'\' LIMIT 1');

SELECT '-- NOT must be followed by LIKE or ILIKE';
SELECT formatQuerySingleLine('SHOW DATABASES NOT'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLineOrNull('SHOW DATABASES NOT') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW CLUSTERS NOT') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW MERGES NOT') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW TABLES NOT') IS NULL;

SELECT '-- LIKE and ILIKE need a string literal';
SELECT formatQuerySingleLineOrNull('SHOW DATABASES LIKE 123') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW CLUSTERS LIKE 123') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW MERGES LIKE 123') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW SETTINGS LIKE 123') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW TABLES LIKE 123') IS NULL;

SELECT '-- LIMIT, WHERE, FROM, CLUSTER and CHANGED need their argument';
SELECT formatQuerySingleLineOrNull('SHOW DATABASES LIKE \'a\' LIMIT') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW CLUSTERS LIKE \'a\' LIMIT') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW MERGES LIKE \'a\' LIMIT') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW TABLES LIMIT') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW TABLES WHERE') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW TABLES FROM 1') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW CLUSTER 123') IS NULL;
SELECT formatQuerySingleLineOrNull('SHOW CHANGED LIKE \'a\'') IS NULL;
