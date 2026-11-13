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
