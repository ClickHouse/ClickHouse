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

CREATE TABLE squality_alias_scope_outer (y UInt64) ENGINE = Memory;
CREATE TABLE squality_alias_scope_array (x Array(UInt64)) ENGINE = Memory;
INSERT INTO squality_alias_scope_outer VALUES (4);
INSERT INTO squality_alias_scope_array VALUES ([1, 2]);

-- A parent table alias participates in the same nearest-scope lookup as an original table name.
SELECT y, (SELECT squality_alias_scope_wildcard.* FROM squality_alias_scope_wildcard AS inner_table LIMIT 1)
FROM squality_alias_scope_outer AS squality_alias_scope_wildcard;

-- Correlated matcher columns use the owning scope's post-ARRAY JOIN column.
SELECT x, (SELECT squality_alias_scope_array.* FROM squality_alias_scope_array AS inner_table LIMIT 1)
FROM squality_alias_scope_array ARRAY JOIN x
ORDER BY x;

-- Correlated matcher columns receive the owning scope's nullable grouping-key transformation.
SET group_by_use_nulls = 1;
SELECT
    toTypeName(squality_alias_scope_wildcard.x),
    (SELECT toTypeName(squality_alias_scope_wildcard.*) FROM squality_alias_scope_wildcard AS inner_table LIMIT 1)
FROM squality_alias_scope_wildcard
GROUP BY ROLLUP(squality_alias_scope_wildcard.x)
HAVING GROUPING(squality_alias_scope_wildcard.x) = 0
ORDER BY squality_alias_scope_wildcard.x NULLS FIRST;

CREATE TABLE squality_alias_scope_semi_left (x UInt64) ENGINE = Memory;
CREATE TABLE squality_alias_scope_semi_right (x UInt64) ENGINE = Memory;
INSERT INTO squality_alias_scope_semi_left VALUES (1);
INSERT INTO squality_alias_scope_semi_right VALUES (1), (2), (3);

SET semi_join_include_columns_from_both_sides = 0;

SELECT (SELECT sum(squality_alias_scope_semi_right.x) FROM squality_alias_scope_semi_right AS inner_table)
FROM squality_alias_scope_semi_left LEFT SEMI JOIN squality_alias_scope_semi_right
    ON squality_alias_scope_semi_left.x = squality_alias_scope_semi_right.x;

-- Correlation is collected from column descendants when owning-scope resolution produces an expression.
CREATE TABLE squality_alias_scope_nested (n Nested(x UInt64, y String)) ENGINE = Memory;
INSERT INTO squality_alias_scope_nested VALUES ([1, 2], ['a', 'b']);

SELECT n.x, (SELECT tuple(squality_alias_scope_nested.*) FROM squality_alias_scope_nested AS inner_table LIMIT 1)
FROM squality_alias_scope_nested ARRAY JOIN n
ORDER BY n.x;

-- Matcher post-processing must not substitute an identically shaped grouping key from the inner scope.
SELECT x, (
    SELECT squality_alias_scope_wildcard.*
    FROM squality_alias_scope_wildcard AS inner_table
    WHERE inner_table.x = 3
    GROUP BY ROLLUP(inner_table.x)
    HAVING GROUPING(inner_table.x) = 0
    LIMIT 1
)
FROM squality_alias_scope_wildcard
ORDER BY x; -- { serverError NOT_AN_AGGREGATE }

-- Discarded columns are neither resolved nor registered as correlated.
CREATE TABLE squality_alias_scope_dict_src (id UInt64, v String) ENGINE = Memory;
CREATE DICTIONARY squality_alias_scope_dict
    (id UInt64, v String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'squality_alias_scope_dict_src'))
    LAYOUT(FLAT())
    LIFETIME(0);
CREATE TABLE squality_alias_scope_alias
(
    id UInt64,
    x UInt64,
    al String ALIAS dictGet('squality_alias_scope_dict', 'v', id)
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO squality_alias_scope_alias VALUES (1, 10);
DROP DICTIONARY squality_alias_scope_dict SETTINGS check_table_dependencies = 0;

SET asterisk_include_alias_columns = 1;
SELECT id, (SELECT tuple(squality_alias_scope_alias.* EXCEPT (al))
            FROM squality_alias_scope_alias AS inner_table LIMIT 1)
FROM squality_alias_scope_alias;

-- Hiding the original name of an aliased table must also affect JOIN-side pruning. Otherwise the
-- hidden name prunes the sibling table whose same-named Tuple subcolumn is the actual local binding.
CREATE TABLE squality_alias_scope_join_name (x UInt64) ENGINE = Memory;
CREATE TABLE squality_alias_scope_join_tuple
    (squality_alias_scope_join_name Tuple(x UInt64)) ENGINE = Memory;
INSERT INTO squality_alias_scope_join_name VALUES (1), (2), (3);
INSERT INTO squality_alias_scope_join_tuple VALUES ((100));

SET analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT x, (
    SELECT squality_alias_scope_join_name.x
    FROM squality_alias_scope_join_name AS inner_table
    INNER JOIN squality_alias_scope_join_tuple ON 1
    LIMIT 1
)
FROM squality_alias_scope_join_name
ORDER BY x;

SELECT x, (
    SELECT squality_alias_scope_join_name.x
    FROM squality_alias_scope_join_name AS inner_table
    CROSS JOIN squality_alias_scope_join_tuple
    LIMIT 1
)
FROM squality_alias_scope_join_name
ORDER BY x;

-- A hidden SEMI/ANTI JOIN side must not claim an original table name that the alias hides. The
-- single-part, database-qualified and matcher forms below all bind to the outer table instead.
SET anti_join_include_columns_from_both_sides = 0;

SELECT x, (
    SELECT squality_alias_scope_join_name.x
    FROM numbers(1, 1) AS preserved
    LEFT SEMI JOIN squality_alias_scope_join_name AS hidden_table
        ON preserved.number = hidden_table.x
    LIMIT 1
)
FROM squality_alias_scope_join_name
ORDER BY x;

SELECT x, (
    SELECT {CLICKHOUSE_DATABASE:Identifier}.squality_alias_scope_join_name.x
    FROM squality_alias_scope_join_name AS hidden_table
    RIGHT SEMI JOIN numbers(1, 1) AS preserved
        ON preserved.number = hidden_table.x
    LIMIT 1
)
FROM squality_alias_scope_join_name
ORDER BY x;

SELECT x, (
    SELECT squality_alias_scope_join_name.*
    FROM numbers(1) AS preserved
    LEFT ANTI JOIN squality_alias_scope_join_name AS hidden_table
        ON preserved.number = hidden_table.x
    LIMIT 1
)
FROM squality_alias_scope_join_name
ORDER BY x;

-- An alias equal to the database name only binds the one-part name. It does not make the
-- database-qualified original table name visible in the inner scope.
CREATE DATABASE squality_alias_scope_db;
CREATE TABLE squality_alias_scope_db.t (x UInt64) ENGINE = Memory;
INSERT INTO squality_alias_scope_db.t VALUES (1), (2), (3);

SELECT count() FROM squality_alias_scope_db.t
WHERE EXISTS
    (SELECT 1 FROM squality_alias_scope_db.t AS squality_alias_scope_db
     WHERE squality_alias_scope_db.x > squality_alias_scope_db.t.x);

SELECT x, (
    SELECT squality_alias_scope_db.t.*
    FROM squality_alias_scope_db.t AS squality_alias_scope_db
    WHERE squality_alias_scope_db.x = 3
    LIMIT 1
)
FROM squality_alias_scope_db.t
ORDER BY x;

DROP DATABASE squality_alias_scope_db;

SET allow_correlated_subqueries = 0;
SELECT (SELECT squality_alias_scope_wildcard.* REPLACE (1 AS x)
        FROM squality_alias_scope_wildcard AS inner_table LIMIT 1)
FROM squality_alias_scope_wildcard LIMIT 1;
SELECT count(), (SELECT squality_alias_scope_wildcard.* REPLACE (1 AS x)
                 FROM squality_alias_scope_wildcard AS inner_table LIMIT 1)
FROM squality_alias_scope_wildcard;

DROP TABLE squality_alias_scope;
DROP TABLE squality_alias_scope_wildcard;
DROP TABLE squality_alias_scope_outer;
DROP TABLE squality_alias_scope_array;
DROP TABLE squality_alias_scope_semi_left;
DROP TABLE squality_alias_scope_semi_right;
DROP TABLE squality_alias_scope_nested;
DROP TABLE squality_alias_scope_alias;
DROP TABLE squality_alias_scope_dict_src;
DROP TABLE squality_alias_scope_join_name;
DROP TABLE squality_alias_scope_join_tuple;
