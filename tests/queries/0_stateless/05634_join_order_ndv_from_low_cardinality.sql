-- The number of distinct values of a `LowCardinality` column without statistics is taken from the dictionaries of the
-- parts for join reordering, see the setting `estimate_distinct_values_from_metadata`.

SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;
SET low_cardinality_max_dictionary_size = 8192;
SET low_cardinality_use_single_dictionary_for_part = 0;

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, s LowCardinality(String), n LowCardinality(Nullable(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0;

-- 300 distinct values, but 90% of the rows have one of them, so the column compresses as if it had a few.
INSERT INTO t SELECT number, if(number % 10 = 0, 'value ' || toString(intHash32(number) % 300), 'value 0'), if(number % 3 = 0, NULL, toString(number % 50))
FROM numbers(100000);

SELECT 'A skewed column: the number of distinct values of the dictionary';
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) BETWEEN 250 AND 350
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.s = b.s);

SELECT 'A Nullable column: the placeholder for NULL is not counted';
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) BETWEEN 45 AND 55
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.n = b.n);

SELECT 'More values than fit into a dictionary: no number from the dictionaries';
DROP TABLE IF EXISTS w;
CREATE TABLE w (id UInt64, s LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO w SELECT number, toString(intHash64(number)) FROM numbers(100000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 10000
FROM (EXPLAIN SELECT count() FROM w AS a JOIN w AS b ON a.s = b.s);

DROP TABLE t;
DROP TABLE w;
