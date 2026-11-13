-- The number of distinct values of a column without statistics is estimated from its compressed size
-- for join reordering, see the setting `estimate_distinct_values_from_metadata`.

SET use_statistics = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS facts;
DROP TABLE IF EXISTS facts_delta;
DROP TABLE IF EXISTS dim;

-- `k` has 16 equally frequent values. No statistics, wide parts and a general purpose codec, so that the size of the column is known.
CREATE TABLE facts (id UInt32 CODEC(LZ4), k UInt32 CODEC(LZ4), fk UInt32 CODEC(LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;

-- The same data, compressed by the order of the values.
CREATE TABLE facts_delta (id UInt32, k UInt32 CODEC(Delta, LZ4), fk UInt32 CODEC(Delta, LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;

-- A dimension table with a unique key, which does not compress.
CREATE TABLE dim (key UInt32 CODEC(LZ4), name String)
ENGINE = MergeTree ORDER BY name
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;

-- `fk` is a foreign key into `dim`: 90% of the rows have the key 0, so it compresses well.
INSERT INTO facts SELECT number, intHash32(number) % 16, if(number % 10 = 0, intHash32(number) % 100000, 0) FROM numbers(100000);
INSERT INTO facts_delta SELECT * FROM facts;
INSERT INTO dim SELECT number, toString(intHash64(number)) FROM numbers(100000);

-- The selectivity and the output rows of the single join in a plan.
DROP VIEW IF EXISTS join_estimate;
CREATE VIEW join_estimate AS
    SELECT
        toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) AS selectivity,
        extract(arrayStringConcat(groupArray(explain), '\n'), 'Output rows: estimated ([^\n]+)') AS output_rows
    FROM (EXPLAIN SELECT count() FROM facts AS a JOIN facts AS b ON a.k = b.k);

SELECT 'Join on a column with 16 distinct values: the selectivity is about 1/16, so the join is much larger than its inputs.';
SELECT selectivity BETWEEN 1 / 1024 AND 1 / 4 FROM join_estimate;
SELECT selectivity BETWEEN 1 / 1024 AND 1 / 4 FROM join_estimate SETTINGS estimate_distinct_values_from_metadata = 0;

SELECT 'The same with Delta: no estimate, the join is taken as a join with a unique key.';
SELECT toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) < 1 / 1024
FROM (EXPLAIN SELECT count() FROM facts_delta AS a JOIN facts_delta AS b ON a.k = b.k);

SELECT 'Join of a skewed foreign key with a unique key without an estimate: the join is as large as the foreign key side.';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Join \\(JOIN[^\n]*\n[^\n]*\n[^\n]*\n[^\n]*\n[^\n]*\n[^\n]*Output rows: estimated ([^\n]+)')
FROM (EXPLAIN SELECT count() FROM facts AS f JOIN dim AS d ON f.fk = d.key);

DROP VIEW join_estimate;
DROP TABLE facts;
DROP TABLE facts_delta;
DROP TABLE dim;
