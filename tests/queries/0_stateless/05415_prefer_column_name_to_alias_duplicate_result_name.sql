-- With prefer_column_name_to_alias = 1 an unaliased identifier reads the source column even if a
-- projection alias has the same name. Such a column and a different expression must get different
-- result names: https://github.com/ClickHouse/ClickHouse/issues/126525

SET prefer_column_name_to_alias = 1;

SELECT 'issue';
SELECT 46 * col1 AS col2, col2 FROM (SELECT 1 AS col1, 2 AS col2) AS src FORMAT TSVWithNames;

SELECT 'reversed order';
SELECT col2, 46 * col1 AS col2 FROM (SELECT 1 AS col1, 2 AS col2) AS src FORMAT TSVWithNames;

SELECT 'same type, different constants';
SELECT 46 * col1 AS col2, col2 FROM (SELECT 1 AS col1, toUInt16(2) AS col2) FORMAT TSVWithNames;

SELECT 'alias of another column';
SELECT col1 AS col2, col2 FROM (SELECT 1 AS col1, 'a' AS col2) FORMAT TSVWithNames;

SELECT 'alias of an expression of the column itself';
SELECT x + 1 AS x, x FROM (SELECT 1 AS x) FORMAT TSVWithNames;

SELECT 'wrapped';
SELECT * FROM (SELECT toUInt64(46 * col1) AS col2, col2 FROM (SELECT number + 1 AS col1, number + 2 AS col2 FROM numbers(2)) AS src) ORDER BY ALL FORMAT TSVWithNames;

SELECT 'remote';
DROP TABLE IF EXISTS t_05415;
CREATE TABLE t_05415 (col1 UInt8, col2 UInt8) ENGINE = Memory;
INSERT INTO t_05415 VALUES (1, 2);
SELECT 46 * col1 AS col2, col2 FROM remote('127.0.0.1', currentDatabase(), t_05415) FORMAT TSVWithNames SETTINGS prefer_localhost_replica = 0;
SELECT 46 * col1 AS col2, col2 FROM remote('127.0.0.1', currentDatabase(), t_05415) FORMAT TSVWithNames SETTINGS prefer_localhost_replica = 1;
SELECT toUInt8(46 * col1) AS col2, col2 FROM remote('127.0.0.1', currentDatabase(), t_05415) FORMAT TSVWithNames SETTINGS prefer_localhost_replica = 0;
SELECT toUInt8(46 * col1) AS col2, col2 FROM remote('127.0.0.1', currentDatabase(), t_05415) FORMAT TSVWithNames SETTINGS prefer_localhost_replica = 1;
DROP TABLE t_05415;

-- Each JOIN query is read twice, directly and through SELECT *: both must return the same values.
SELECT 'LEFT JOIN USING, the alias names the other side';
SELECT b.k AS k, k FROM (SELECT 1 AS k) AS a LEFT JOIN (SELECT 2 AS k) AS b USING (k) ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM (SELECT b.k AS k, k FROM (SELECT 1 AS k) AS a LEFT JOIN (SELECT 2 AS k) AS b USING (k)) ORDER BY ALL FORMAT TSVWithNames;

SELECT 'FULL JOIN USING';
SELECT a.k AS k, k FROM (SELECT 1 AS k) AS a FULL OUTER JOIN (SELECT 2 AS k) AS b USING (k) ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM (SELECT a.k AS k, k FROM (SELECT 1 AS k) AS a FULL OUTER JOIN (SELECT 2 AS k) AS b USING (k)) ORDER BY ALL FORMAT TSVWithNames;

SELECT 'self-join of one table';
DROP TABLE IF EXISTS t8_05415;
CREATE TABLE t8_05415 (k UInt8, v UInt8) ENGINE = Memory;
INSERT INTO t8_05415 VALUES (1, 1), (2, 0), (3, 1);
SELECT b.k AS k, k FROM t8_05415 AS a LEFT JOIN t8_05415 AS b ON a.k = b.k AND b.v = 1 ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM (SELECT b.k AS k, k FROM t8_05415 AS a LEFT JOIN t8_05415 AS b ON a.k = b.k AND b.v = 1) ORDER BY ALL FORMAT TSVWithNames;
DROP TABLE t8_05415;

SELECT 'self-join of one table, Nested prefix';
SET flatten_nested = 1;
DROP TABLE IF EXISTS t9_05415;
CREATE TABLE t9_05415 (k UInt8, v UInt8, n Nested(a UInt8)) ENGINE = Memory;
INSERT INTO t9_05415 VALUES (1, 1, [1]), (2, 0, [2]), (3, 1, [3]);
SELECT b.n AS n, n FROM t9_05415 AS a LEFT JOIN t9_05415 AS b ON a.k = b.k AND b.v = 1 ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM (SELECT b.n AS n, n FROM t9_05415 AS a LEFT JOIN t9_05415 AS b ON a.k = b.k AND b.v = 1) ORDER BY ALL FORMAT TSVWithNames;
DROP TABLE t9_05415;

-- The original rows keep x + 1 in x; the inserted row interpolates only the renamed columns.
SELECT 'INTERPOLATE of a renamed column';
SELECT 2 * number AS k, x + 1 AS x, x FROM (SELECT number, number AS x FROM numbers(2)) ORDER BY k WITH FILL INTERPOLATE (x AS x + 10) FORMAT TSVWithNames;
SELECT 2 * number AS k, x + 1 AS x, x, x FROM (SELECT number, number AS x FROM numbers(2)) ORDER BY k WITH FILL INTERPOLATE (x AS x + 10) FORMAT TSVWithNames;
SELECT 2 * number AS k, number + 1 AS number, number FROM remote('127.0.0.1', numbers(2)) ORDER BY k WITH FILL INTERPOLATE (number AS number + 10) FORMAT TSVWithNames SETTINGS prefer_localhost_replica = 0;

SELECT 'the same column twice keeps its name';
SELECT col2 AS col2, col2 FROM (SELECT 1 AS col1, 2 AS col2) FORMAT TSVWithNames;

SELECT 'prefer_column_name_to_alias = 0 reads the alias';
SELECT 46 * col1 AS col2, col2 FROM (SELECT 1 AS col1, 2 AS col2) AS src FORMAT TSVWithNames SETTINGS prefer_column_name_to_alias = 0;

SELECT 'conflicting aliases are still an error';
SELECT 1 AS x, 2 AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
