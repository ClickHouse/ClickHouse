-- Tags: shard

-- With aggregate functions, `GROUP BY ()` is the empty grouping set: one row even over an empty input.
-- https://github.com/ClickHouse/ClickHouse/issues/130185

CREATE TABLE inner_values (i Int32) ENGINE = Memory;
INSERT INTO inner_values VALUES (1);

SELECT 'issue', coalesce(sum(i), 42) FROM inner_values WHERE i = 2 GROUP BY () HAVING true;
SELECT 'count', count() FROM numbers(3) WHERE number > 5 GROUP BY ();
SELECT 'tuple()', count() FROM numbers(3) WHERE number > 5 GROUP BY tuple();
SELECT 'two empty tuples', count() FROM numbers(3) WHERE number > 5 GROUP BY (), ();
SELECT 'aggregate in HAVING', 1 FROM numbers(3) WHERE number > 5 GROUP BY () HAVING count() = 0;
SELECT 'aggregate in ORDER BY', 1 FROM numbers(3) WHERE number > 5 GROUP BY () ORDER BY count();
SELECT 'totals', count() FROM numbers(3) WHERE number > 5 GROUP BY () WITH TOTALS;
SELECT 'subquery', x FROM (SELECT 1 AS x, count() FROM numbers(3) WHERE number > 5 GROUP BY ());
SELECT 'exists', exists(SELECT count() FROM numbers(3) WHERE number > 5 GROUP BY ());
SELECT 'scalar', (SELECT count() FROM numbers(3) WHERE number > 5 GROUP BY ());
SELECT 'non-empty', count() FROM numbers(3) GROUP BY ();
SELECT 'keyed', count() FROM (SELECT count() FROM numbers(3) WHERE number > 5 GROUP BY (), number);
SELECT 'constant key', count() FROM (SELECT count() FROM numbers(3) WHERE number > 5 GROUP BY 'a');
SELECT 'cast', count() FROM (SELECT count() FROM numbers(3) WHERE number > 5 GROUP BY CAST(tuple() AS Tuple()));
SELECT 'grouping', grouping(()), count() FROM numbers(3) GROUP BY ();
SELECT 'suppressed', count() FROM (SELECT count() FROM numbers(3) WHERE number > 5 GROUP BY () SETTINGS empty_result_for_aggregation_by_empty_set = 1);
SELECT 'no aggregate, non-empty', 1 FROM numbers(3) GROUP BY ();
SELECT 'query tree', count() FROM (EXPLAIN QUERY TREE SELECT count() FROM numbers(3) GROUP BY ()) WHERE explain ILIKE '%GROUP BY%';

CREATE TABLE t_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_mt SELECT number FROM numbers(10);
INSERT INTO t_mt SELECT number FROM numbers(10, 10);
SELECT 'several streams', count(), sum(x) FROM t_mt WHERE x > 100 GROUP BY () SETTINGS max_threads = 4;

SELECT 'distributed', count() FROM remote('127.0.0.{1,2}', numbers(3)) WHERE number > 5 GROUP BY ();
SELECT 'distributed, non-empty', count() FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY ();
SELECT 'grouping, distributed', grouping(()), count() FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY ();

DROP TABLE t_mt;
DROP TABLE inner_values;
