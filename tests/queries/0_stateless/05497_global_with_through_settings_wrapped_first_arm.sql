-- The `WITH` clause of the first operand of a set operation applies to the other operands too,
-- also when the first operand is parenthesized and has its own `SETTINGS` clause.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = Memory;
INSERT INTO t VALUES (99);

SELECT 'cte shadowing a table, union all';
SELECT * FROM ((WITH t AS (SELECT toUInt8(1) AS x) SELECT x FROM t SETTINGS max_threads = 1) UNION ALL SELECT x FROM t) ORDER BY ALL;

SELECT 'cte shadowing a table, union distinct';
SELECT * FROM ((WITH t AS (SELECT toUInt8(1) AS x) SELECT x FROM t SETTINGS max_threads = 1) UNION DISTINCT SELECT x FROM t) ORDER BY ALL;

SELECT 'scalar alias';
SELECT * FROM ((WITH 1 AS x SELECT x SETTINGS max_threads = 1) UNION ALL SELECT x) ORDER BY ALL;

SELECT 'top level';
(WITH 2 AS x SELECT x SETTINGS max_threads = 1) UNION DISTINCT SELECT x;

DROP TABLE t;
