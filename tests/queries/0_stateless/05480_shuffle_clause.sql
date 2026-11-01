-- The `SHUFFLE` clause returns the rows in a random order. The tests check that the set of rows is preserved
-- (the order is random, so the results are checked in an order-independent way).

-- The clause is experimental.
SET allow_experimental_shuffle_query = 0;
SELECT number FROM numbers(10) SHUFFLE; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_shuffle_query = 1;

SELECT 'Syntax';
SELECT formatQuerySingleLine('SELECT number FROM numbers(10) SHUFFLE LIMIT 3');
SELECT formatQuerySingleLine('SELECT number FROM numbers(10) AS shuffle SHUFFLE');
SELECT number FROM numbers(10) ORDER BY number SHUFFLE; -- { clientError SYNTAX_ERROR }
SELECT number FROM numbers(10) SHUFFLE LIMIT 3 WITH TIES; -- { clientError WITH_TIES_WITHOUT_ORDER_BY }
-- `SHUFFLE` is not a table alias.
SELECT count() FROM numbers(10) SHUFFLE;
SELECT count() FROM numbers(10) AS shuffle SHUFFLE;

SELECT 'Full shuffle preserves the rows';
SELECT count(), sum(number), groupBitXor(sipHash64(number)) = (SELECT groupBitXor(sipHash64(number)) FROM numbers(100000))
FROM (SELECT number FROM numbers(100000) SHUFFLE);
SELECT count(), sum(number) FROM (SELECT number FROM numbers_mt(100000) SHUFFLE SETTINGS max_threads = 8, max_block_size = 1000);
SELECT arraySort(groupArray(number)) = range(1000) FROM (SELECT number FROM numbers_mt(1000) SHUFFLE SETTINGS max_threads = 4, max_block_size = 17);

SELECT 'Empty';
SELECT count() FROM (SELECT number FROM numbers(10) WHERE number > 100 SHUFFLE);
SELECT count() FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 0);

SELECT 'LIMIT';
SELECT count(), uniqExact(number), max(number) < 100000 FROM (SELECT number FROM numbers_mt(100000) SHUFFLE LIMIT 1000 SETTINGS max_threads = 8, max_block_size = 1000);
SELECT count(), uniqExact(number) FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 100);
SELECT count(), uniqExact(number) FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 1);
SELECT count() FROM (SELECT number FROM numbers(100) SHUFFLE LIMIT 10 OFFSET 95);
SELECT count() FROM (SELECT number FROM numbers(100) SHUFFLE LIMIT 95, 10);
SELECT count() FROM (SELECT number FROM numbers(100) SHUFFLE OFFSET 90);
SELECT count() FROM (SELECT number FROM numbers(100) SHUFFLE LIMIT 0.1);

SELECT 'Other clauses';
SELECT count(), uniqExact(x) FROM (SELECT number % 10 AS x FROM numbers(1000) SHUFFLE LIMIT 2 BY x);
SELECT arraySort(groupArray(x)) FROM (SELECT DISTINCT number % 5 AS x FROM numbers(1000) SHUFFLE);
SELECT arraySort(groupArray((k, c))) FROM (SELECT number % 3 AS k, count() AS c FROM numbers(30) GROUP BY k SHUFFLE);
SELECT k, c FROM (SELECT number % 3 AS k, count() AS c FROM numbers(30) GROUP BY k WITH TOTALS SHUFFLE) ORDER BY k;
SELECT count(), sum(rn) FROM (SELECT number, row_number() OVER () AS rn FROM numbers(100) SHUFFLE);
SELECT x, s FROM (SELECT 'a' AS x, toString(number) AS s FROM numbers(3) SHUFFLE) ORDER BY s;

SELECT 'Subqueries';
SELECT count() FROM (SELECT * FROM (SELECT number FROM numbers(100) SHUFFLE) SHUFFLE LIMIT 7);
SELECT sum(number) FROM numbers(100) WHERE number IN (SELECT number FROM numbers(50) SHUFFLE LIMIT 50);
SELECT count() FROM (SELECT number FROM numbers(10) SHUFFLE UNION ALL SELECT number FROM numbers(10) SHUFFLE);

SELECT 'Distributed';
SELECT count(), sum(number) FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE);
SELECT count(), uniqExact(number) FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE LIMIT 10);
SELECT count(), sum(c) FROM (SELECT number % 7 AS k, count() AS c FROM remote('127.0.0.{1,2}', numbers(1000)) GROUP BY k SHUFFLE);
SELECT count() FROM (SELECT number FROM remote('127.0.0.{1,2}', numbers(1000)) SHUFFLE LIMIT 10 OFFSET 1995);

SELECT 'MergeTree';
DROP TABLE IF EXISTS t_shuffle;
CREATE TABLE t_shuffle (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_shuffle SELECT number, toString(number) FROM numbers(10000);
SELECT count(), sum(id), countIf(s != toString(id)) FROM (SELECT id, s FROM t_shuffle SHUFFLE);
SELECT count(), uniqExact(id), countIf(s != toString(id)) FROM (SELECT id, s FROM t_shuffle SHUFFLE LIMIT 100);
SELECT count(), uniqExact(id), countIf(s != toString(id)) FROM (SELECT id, s FROM t_shuffle WHERE id % 2 = 0 SHUFFLE LIMIT 100);

SELECT 'Plan';
SELECT trimLeft(explain) FROM (EXPLAIN SELECT id FROM t_shuffle SHUFFLE) WHERE explain ILIKE '%shuffle%';
-- The rest of the columns is read only for the selected rows.
SELECT count() > 0 FROM (EXPLAIN SELECT id, s FROM t_shuffle SHUFFLE LIMIT 10 SETTINGS query_plan_optimize_lazy_materialization = 1) WHERE explain LIKE '%LazilyRead%';
DROP TABLE t_shuffle;
