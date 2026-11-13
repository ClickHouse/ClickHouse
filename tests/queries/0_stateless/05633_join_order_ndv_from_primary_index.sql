-- The numbers of distinct values of the columns of the primary key without statistics are estimated from the
-- primary key index for join reordering, see the setting `estimate_distinct_values_from_metadata`.

SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t;

-- `k` has 1000 values, each in a run of 200 rows, longer than a granule of 64 rows; `v` has 100 values.
CREATE TABLE t (k UInt32, v UInt32, x UInt64)
ENGINE = MergeTree ORDER BY (k, v)
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi',
    primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1;

INSERT INTO t SELECT intDiv(number, 200), intHash32(number) % 100, number FROM numbers(200000);

SELECT 'The first column of the key';
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) BETWEEN 500 AND 2000
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.k = b.k);

SELECT 'The second column of the key';
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) BETWEEN 30 AND 300
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.v = b.v);

SELECT 'A unique first column of the key changes in every granule: no estimate';
DROP TABLE IF EXISTS u;
CREATE TABLE u (id UInt64, x UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi';
INSERT INTO u SELECT number, intHash64(number) FROM numbers(100000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) >= 50000
FROM (EXPLAIN SELECT count() FROM u AS a JOIN u AS b ON a.id = b.id);

DROP TABLE t;
DROP TABLE u;
