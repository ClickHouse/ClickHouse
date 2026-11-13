-- Tags: no-replicated-database
-- Tag no-replicated-database: a refreshable view with a non-replicated target is refused on a Replicated database.
-- Random settings limits: optimize_functions_to_subcolumns=(1, 1)
-- optimize_functions_to_subcolumns: it is what reads length(arr) as arr.size0.

-- RENAME COLUMN of a column that a materialized view reads is refused, like DROP COLUMN.
CREATE TABLE t (k Int32, a Int32, b Int32, c Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dst (k Int32, a Int32, b Int32) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT k, a, b FROM t;
ALTER TABLE t RENAME COLUMN b TO b2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t RENAME COLUMN c TO c2;
INSERT INTO t VALUES (1, 1, 1, 1);
SELECT (SELECT count() FROM t), (SELECT count() FROM dst);

-- A column read only in WHERE, by a view with an inner table.
CREATE TABLE t2 (k Int32, a Int32, f UInt8) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv2 ENGINE = MergeTree ORDER BY k AS SELECT k, a FROM t2 WHERE f;
ALTER TABLE t2 RENAME COLUMN f TO f2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- SELECT * reads every column.
CREATE TABLE t3 (k Int32, a Int32) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv3 ENGINE = MergeTree ORDER BY k AS SELECT * FROM t3;
ALTER TABLE t3 RENAME COLUMN a TO a2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- Columns the query names that analysis or an optimization removes: an unused column of a subquery,
-- length(arr) read as arr.size0, and a column of a scalar subquery folded into a constant.
CREATE TABLE t7 (k Int32, b Int32, c Int32, d Int32, arr Array(Int32)) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv7 ENGINE = MergeTree ORDER BY l
    AS SELECT length(arr) AS l, (SELECT groupArray(c) FROM t7) AS m FROM (SELECT b, arr FROM t7);
ALTER TABLE t7 RENAME COLUMN b TO b2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t7 DROP COLUMN b; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t7 RENAME COLUMN arr TO arr2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t7 RENAME COLUMN c TO c2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t7 RENAME COLUMN d TO d2;
INSERT INTO t7 VALUES (1, 1, 1, 1, [1, 2]);
SELECT l FROM mv7;

-- Columns read only by a named window, a subcolumn, or a scalar subquery folded by the view's own settings.
CREATE TABLE t8 (k Int32, b Int32, c Int32, tup Tuple(x Int32)) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv8 ENGINE = Memory AS SELECT k FROM t8 WINDOW w AS (PARTITION BY b);
CREATE MATERIALIZED VIEW mv8s ENGINE = Memory
    AS SELECT k, (SELECT groupArray(c) FROM t8) AS cs, tup.x FROM t8 SETTINGS enable_scalar_subquery_optimization = 1;
ALTER TABLE t8 RENAME COLUMN b TO b2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t8 RENAME COLUMN c TO c2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t8 RENAME COLUMN tup TO tup2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The rename rewrites an ALIAS column that the views read, so they keep working.
CREATE TABLE t9 (k Int32, b Int32, a String ALIAS toTypeName(b)) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv9 ENGINE = Memory AS SELECT k, a FROM t9;
CREATE MATERIALIZED VIEW mv9s ENGINE = Memory AS SELECT k, (SELECT groupArray(a) FROM t9) AS ga FROM t9;
ALTER TABLE t9 RENAME COLUMN b TO b2;
INSERT INTO t9 VALUES (1, 1);
SELECT a FROM mv9;
CREATE TABLE t12 (k Int32, b Int32, a Int32 ALIAS b, aa Int32 ALIAS a) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv12 ENGINE = Memory AS SELECT k, aa FROM t12;
ALTER TABLE t12 RENAME COLUMN a TO a2;

-- Renamed, the column a folded scalar subquery reads would resolve to an alias of the query.
CREATE TABLE t11 (k Int32, b Int32) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv11 ENGINE = Memory AS WITH toInt32(0) AS b SELECT k, (SELECT groupArray(b) FROM t11) AS bs FROM t11
    SETTINGS prefer_column_name_to_alias = 1, enable_scalar_subquery_optimization = 1;
ALTER TABLE t11 RENAME COLUMN b TO b2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- A command renames a column that an earlier command of the same ALTER adds.
CREATE TABLE t10 (k Int32, `n.a` UInt32) ENGINE = MergeTree ORDER BY k SETTINGS share_nested_offsets = 0;
CREATE MATERIALIZED VIEW mv10 ENGINE = Memory AS SELECT k FROM t10;
ALTER TABLE t10 ADD COLUMN IF NOT EXISTS n String, RENAME COLUMN n TO n2;

-- A table function argument computed from the source is evaluated on an empty block of it.
CREATE TABLE t13 (k Int32, x Int32) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv13 ENGINE = Memory AS SELECT s.k FROM t13 AS s CROSS JOIN numbers(assumeNotNull((SELECT count() FROM t13))) AS n;
ALTER TABLE t13 RENAME COLUMN x TO x2;
ALTER TABLE t13 DROP COLUMN x2;
-- ... or fails for an empty block: whether the query names the column decides.
CREATE TABLE t15 (k Int32, x Int32, y Int32, z Int32, nest Nested(x Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t15 VALUES (1, 1, 1, 1, [1]);
CREATE MATERIALIZED VIEW mv15 ENGINE = Memory AS SELECT s.k, (SELECT max(y) FROM t15) AS m, tupleElement(s.nest, 'x') AS nx
    FROM t15 AS s CROSS JOIN numbers(assumeNotNull((SELECT intDiv(10, count()) FROM t15))) AS n WINDOW w AS (PARTITION BY s.z);
ALTER TABLE t15 RENAME COLUMN x TO s; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t15 RENAME COLUMN x TO x2;
ALTER TABLE t15 DROP COLUMN x2;
ALTER TABLE t15 RENAME COLUMN k TO k2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t15 RENAME COLUMN y TO y2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t15 DROP COLUMN y; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t15 RENAME COLUMN z TO z2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t15 DROP COLUMN z; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t15 RENAME COLUMN `nest.x` TO `nest.y`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
INSERT INTO t15 VALUES (2, 2, 2, [2]);
SELECT count() FROM mv15;
-- A view that fails on the stored rows too is broken: its error is returned.
CREATE TABLE t16 (k Int32, b Int32, c Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv16 ENGINE = Memory AS SELECT k, b FROM t16;
DETACH TABLE mv16;
ALTER TABLE t16 RENAME COLUMN b TO b2;
ATTACH TABLE mv16;
ALTER TABLE t16 DROP COLUMN c; -- { serverError UNKNOWN_IDENTIFIER }

-- A recursive CTE, which each analysis gives a new temporary table.
CREATE TABLE t14 (k Int32, c Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv14 ENGINE = Memory
    AS WITH RECURSIVE r AS (SELECT k AS n FROM t14 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r;
ALTER TABLE t14 RENAME COLUMN c TO c2;
ALTER TABLE t14 DROP COLUMN c2;
ALTER TABLE t14 RENAME COLUMN k TO k2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
INSERT INTO t14 VALUES (1);
SELECT count() FROM mv14;

-- Distributed source.
CREATE TABLE local4 (k Int32, a Int32, b Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dist4 AS local4 ENGINE = Distributed(test_shard_localhost, currentDatabase(), local4);
CREATE MATERIALIZED VIEW mv4 ENGINE = MergeTree ORDER BY k AS SELECT k, a FROM dist4;
ALTER TABLE dist4 RENAME COLUMN a TO a2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE dist4 RENAME COLUMN b TO b2;

-- A refreshable view appending to the source of mv: its rows are still pushed to dst.
CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 YEAR APPEND TO t EMPTY AS SELECT 2 AS k, 2 AS a, 2 AS b;
SYSTEM REFRESH VIEW rmv;
SYSTEM WAIT VIEW rmv;
SELECT (SELECT count() FROM t), (SELECT count() FROM dst);

-- A refreshable view reading a table is not a dependent view of it, as for DROP COLUMN.
CREATE TABLE t5 (k Int32, a Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dst5 (k Int32, a Int32) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW rmv5 REFRESH EVERY 1 YEAR TO dst5 EMPTY AS SELECT k, a FROM t5;
ALTER TABLE t5 RENAME COLUMN a TO a2;
INSERT INTO t5 VALUES (1, 1);

-- A view reading an Alias depends on the alias, not on its target.
SET allow_experimental_alias_table_engine = 1;
CREATE TABLE src6 (k Int32, b Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE al6 ENGINE = Alias('src6');
CREATE TABLE dst6 (k Int32, b Int32) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv6 TO dst6 AS SELECT k, b FROM al6;
ALTER TABLE al6 RENAME COLUMN b TO b2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE al6 DROP COLUMN b; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
INSERT INTO al6 VALUES (1, 1);
SELECT count() FROM dst6;
