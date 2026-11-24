-- A number of distinct values estimated from the metadata of the data (`estimate_distinct_values_from_metadata`) is
-- an effective one, for the selectivity of an equality. It is not the number of groups of a `GROUP BY` or `DISTINCT`
-- on the column: a skewed column gets about 1, while every rare value makes a group.

SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS skewed;
DROP TABLE IF EXISTS dim;

-- `k` is 0 in 99% of the rows and has 10000 other values, one row each: it compresses to less than a bit per row.
CREATE TABLE skewed (id UInt64, k UInt64 CODEC(LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO skewed SELECT number, if(number % 100 = 0, number, 0) FROM numbers(1000000);

CREATE TABLE dim (key UInt64 CODEC(LZ4), name String)
ENGINE = MergeTree ORDER BY name
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO dim SELECT number, toString(intHash64(number)) FROM numbers(100000);

SELECT 'The column has an estimate from its compressed size, far below its 10001 values';
SELECT toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 100
FROM (EXPLAIN SELECT count() FROM skewed AS a JOIN skewed AS b ON a.k = b.k);

SELECT 'The grouped subquery is not estimated at fewer rows than its 10001 groups';
SELECT toUInt64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'g\\[[a-z_]+~([0-9]+)\\]')) >= 10001
FROM (EXPLAIN SELECT count() FROM (SELECT k FROM skewed GROUP BY k) AS g JOIN dim AS d ON g.k = d.key);

SELECT 'The same with the Cascades optimizer: the aggregation and the DISTINCT';
SET enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_workers_num = 4, explain_query_plan_default = 'legacy';
SELECT arrayMin(arrayMap(x -> toFloat64OrZero(x), extractAll(arrayStringConcat(groupArray(explain), '\n'), ' Aggregating[^\n]*\\(rows: ~([0-9.e+]+)'))) >= 10001
FROM (EXPLAIN estimates = 1 SELECT g.k FROM (SELECT k FROM skewed GROUP BY k) AS g JOIN dim AS d ON g.k = d.key);
SELECT arrayMin(arrayMap(x -> toFloat64OrZero(x), extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Distinct[^\n]*\\(rows: ~([0-9.e+]+)'))) >= 10001
FROM (EXPLAIN estimates = 1 SELECT DISTINCT k FROM skewed);

SET enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE skewed;
DROP TABLE dim;
