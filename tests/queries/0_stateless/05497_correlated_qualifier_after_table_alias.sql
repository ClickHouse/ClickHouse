DROP TABLE IF EXISTS squality_alias_scope;

CREATE TABLE squality_alias_scope (a Int32, b Int32) ENGINE = Memory;
INSERT INTO squality_alias_scope VALUES (11, 22), (13, 22), (12, 21);

-- Keep accepting the original table name after an alias when there is no outer binding.
SELECT squality_alias_scope.a, squality_alias_scope.b
FROM squality_alias_scope AS inner_table
ORDER BY a;

-- Resolve the hidden original name as a correlated reference when an outer table owns it.
SET prefer_outer_table_name_for_correlated_identifier = true;

SELECT a, coalesce(
    (SELECT count() FROM squality_alias_scope AS inner_table WHERE inner_table.b < squality_alias_scope.b), 0)
FROM squality_alias_scope
ORDER BY a;

SELECT a
FROM squality_alias_scope
WHERE EXISTS
    (SELECT 1 FROM squality_alias_scope AS inner_table WHERE inner_table.b < squality_alias_scope.b)
ORDER BY a;

SELECT count()
FROM squality_alias_scope
WHERE EXISTS
    (SELECT 1 FROM squality_alias_scope AS inner_table WHERE inner_table.b < squality_alias_scope.b);

SELECT squality_alias_scope.b, sum(a)
FROM squality_alias_scope
GROUP BY squality_alias_scope.b
HAVING sum(a) =
    (SELECT sum(inner_table.a) FROM squality_alias_scope AS inner_table WHERE squality_alias_scope.b = inner_table.b)
ORDER BY squality_alias_scope.b;

DROP TABLE squality_alias_scope;
