-- Under `standard` name matching, `analyzer_compatibility_prefer_alias_over_subcolumn` binds a qualifier
-- to a JOIN side with the same quote-aware folding as the rest of the resolver.

SET enable_analyzer = 1;
SET column_and_query_name_matching = 'standard';
SET analyzer_compatibility_prefer_alias_over_subcolumn = 1;

DROP TABLE IF EXISTS t_prefer_alias;
CREATE TABLE t_prefer_alias (id Int32, b Tuple(id Int32)) ENGINE = Memory;
INSERT INTO t_prefer_alias VALUES (1, tuple(100));

SELECT '--- unquoted qualifier folds to the right-side alias, not the left-side subcolumn';
SELECT a.id, b.id FROM t_prefer_alias AS a LEFT JOIN (SELECT id + 0 AS id FROM t_prefer_alias) AS b ON a.id = b.id;
SELECT a.id, B.id FROM t_prefer_alias AS a LEFT JOIN (SELECT id + 0 AS id FROM t_prefer_alias) AS b ON a.id = B.id;

SELECT '--- unquoted qualifier folds to the table name';
SELECT T_PREFER_ALIAS.b.id FROM t_prefer_alias LEFT JOIN (SELECT id + 0 AS id FROM t_prefer_alias) AS b ON T_PREFER_ALIAS.id = B.id;

SELECT '--- double-quoted qualifier stays exact';
SELECT "B".id FROM t_prefer_alias AS a LEFT JOIN (SELECT id + 0 AS id FROM t_prefer_alias) AS b ON a.id = b.id; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE t_prefer_alias;
