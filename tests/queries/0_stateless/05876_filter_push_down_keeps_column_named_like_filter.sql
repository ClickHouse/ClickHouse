-- A filter keeps a column named like its filter column when the whole predicate is pushed down.
DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_mem;
DROP VIEW IF EXISTS v_sorted;
CREATE TABLE t_mt (id UInt32, active Bool) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mt VALUES (1, true), (2, false), (3, true);
CREATE TABLE t_mem (id UInt32, active Bool) ENGINE = Memory;
INSERT INTO t_mem VALUES (1, true), (2, false), (3, true);
CREATE VIEW v_sorted AS SELECT id, active FROM t_mem ORDER BY id;

SELECT active, id FROM t_mt ORDER BY id SETTINGS additional_result_filter = 'active';
SELECT active FROM t_mem ORDER BY id SETTINGS additional_result_filter = 'active';
SELECT DISTINCT active, id FROM t_mem ORDER BY id SETTINGS additional_result_filter = 'active';
SELECT any(active) AS active, id FROM t_mem GROUP BY id ORDER BY id SETTINGS additional_result_filter = 'active';
SELECT t1.active AS active, t1.id AS id FROM t_mem AS t1 INNER JOIN t_mem AS t2 ON t1.id = t2.id ORDER BY id SETTINGS additional_result_filter = 'active';
SELECT toUInt8(number * 2) AS c, number AS a FROM numbers(3) ORDER BY a SETTINGS additional_result_filter = 'c';
SELECT toLowCardinality(toUInt8(number % 2)) AS c, number AS a FROM numbers(4) ORDER BY a SETTINGS additional_result_filter = 'c';
SELECT CAST(if(number = 2, NULL, number % 2 = 1), 'Nullable(Bool)') AS c, number AS a FROM numbers(4) ORDER BY a SETTINGS additional_result_filter = 'c';
SELECT * FROM v_sorted ORDER BY id SETTINGS additional_table_filters = {'v_sorted': 'active'};
SELECT * FROM v_sorted SETTINGS additional_table_filters = {'v_sorted': 'active'};
SELECT (number % 2 = 1) AND (number > 0) AS c, number AS a FROM numbers(4) ORDER BY a SETTINGS additional_result_filter = 'c';
SELECT DISTINCT 1::UInt8 AS c, id FROM t_mem ORDER BY id SETTINGS additional_result_filter = 'c';
SELECT active, id, e FROM t_mem ARRAY JOIN [10, 20] AS e SETTINGS additional_result_filter = 'active';
SELECT e, id FROM t_mem ARRAY JOIN [0, 1, 2]::Array(UInt8) AS e ORDER BY id, e SETTINGS additional_result_filter = 'e';
SELECT e, k FROM (SELECT [1, 0, 2]::Array(UInt8) AS arr, 7 AS k) ARRAY JOIN arr AS e SETTINGS additional_result_filter = 'e';
SELECT countIf(explain ILIKE '%: c UInt8 Const(%') > 0 FROM (EXPLAIN PIPELINE header = 1 SELECT DISTINCT 1::UInt8 AS c, id FROM t_mem ORDER BY id SETTINGS additional_result_filter = 'c');

DROP VIEW v_sorted;
DROP TABLE t_mem;
DROP TABLE t_mt;
