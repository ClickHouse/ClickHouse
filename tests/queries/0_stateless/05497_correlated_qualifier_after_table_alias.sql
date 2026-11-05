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

SELECT count()
FROM {CLICKHOUSE_DATABASE:Identifier}.squality_alias_scope
WHERE EXISTS
    (SELECT 1 FROM {CLICKHOUSE_DATABASE:Identifier}.squality_alias_scope AS inner_table
     WHERE inner_table.b < {CLICKHOUSE_DATABASE:Identifier}.squality_alias_scope.b);

SELECT squality_alias_scope.b, sum(a)
FROM squality_alias_scope
GROUP BY squality_alias_scope.b
HAVING sum(a) =
    (SELECT sum(inner_table.a) FROM squality_alias_scope AS inner_table WHERE squality_alias_scope.b = inner_table.b)
ORDER BY squality_alias_scope.b;

SET enable_materialized_cte = 1;

WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3))
SELECT count() FROM c
WHERE EXISTS (SELECT 1 FROM c AS inner_table WHERE inner_table.x > c.x);

CREATE TABLE squality_alias_scope_wildcard (x UInt64) ENGINE = Memory;
INSERT INTO squality_alias_scope_wildcard VALUES (1), (2), (3);

SELECT x, (SELECT squality_alias_scope_wildcard.* FROM squality_alias_scope_wildcard AS inner_table WHERE inner_table.x = 3 LIMIT 1)
FROM squality_alias_scope_wildcard
ORDER BY x;

CREATE TABLE squality_alias_scope_semi_left (x UInt64) ENGINE = Memory;
CREATE TABLE squality_alias_scope_semi_right (x UInt64) ENGINE = Memory;
INSERT INTO squality_alias_scope_semi_left VALUES (1);
INSERT INTO squality_alias_scope_semi_right VALUES (1), (2), (3);

SET semi_join_include_columns_from_both_sides = 0;

SELECT (SELECT sum(squality_alias_scope_semi_right.x) FROM squality_alias_scope_semi_right AS inner_table)
FROM squality_alias_scope_semi_left LEFT SEMI JOIN squality_alias_scope_semi_right
    ON squality_alias_scope_semi_left.x = squality_alias_scope_semi_right.x;

DROP TABLE squality_alias_scope;
DROP TABLE squality_alias_scope_wildcard;
DROP TABLE squality_alias_scope_semi_left;
DROP TABLE squality_alias_scope_semi_right;
