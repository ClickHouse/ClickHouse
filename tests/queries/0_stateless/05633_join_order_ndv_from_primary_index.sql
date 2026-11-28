-- The numbers of distinct values of the columns of the primary key without statistics are estimated from the
-- primary key index for join reordering, see the setting `estimate_distinct_values_from_metadata`.

SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t;

-- `k` has 1000 values, each in a run of 200 rows, longer than a granule of 64 rows.
-- Only the index in memory is used: without the primary key cache, the index of a new part stays loaded.
CREATE TABLE t (k UInt32, v UInt32, x UInt64)
ENGINE = MergeTree ORDER BY (k, v)
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi',
    primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1, use_primary_key_cache = 0;

INSERT INTO t SELECT intDiv(number, 200), intHash32(number) % 100, number FROM numbers(200000);

SELECT 'The first column of the key';
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) BETWEEN 500 AND 2000
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.k = b.k);

SELECT 'The second column of the key: no estimate, the marks take the first value of each run of the first column';
DROP TABLE IF EXISTS s;
-- `sequence` has 64 values repeated for each `tenant`, and every mark takes `sequence = 0`, in two parts.
CREATE TABLE s (tenant UInt64, sequence UInt64)
ENGINE = MergeTree PARTITION BY intDiv(tenant, 64) ORDER BY (tenant, sequence)
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi',
    primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1;
INSERT INTO s SELECT intDiv(number, 64), number % 64 FROM numbers(8192);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) >= 64
FROM (EXPLAIN SELECT count() FROM s AS a JOIN s AS b ON a.sequence = b.sequence);

SELECT 'A unique first column of the key changes in every granule: no estimate';
DROP TABLE IF EXISTS u;
CREATE TABLE u (id UInt64, x UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi';
INSERT INTO u SELECT number, intHash64(number) FROM numbers(100000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) >= 50000
FROM (EXPLAIN SELECT count() FROM u AS a JOIN u AS b ON a.id = b.id);

SELECT 'A unique first column of the key with a last granule of one row, equal to the final mark: no estimate';
DROP TABLE IF EXISTS u_last;
CREATE TABLE u_last (id UInt64, x UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi';
INSERT INTO u_last SELECT number, intHash64(number) FROM numbers(64 * 1563 + 1);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) >= 50000
FROM (EXPLAIN SELECT count() FROM u_last AS a JOIN u_last AS b ON a.id = b.id);

SELECT 'The same with ZSTD, which compresses a sorted unique column to a few bits per row: no estimate from the compressed size';
DROP TABLE IF EXISTS u_zstd;
CREATE TABLE u_zstd (id UInt64 CODEC(ZSTD(1)), x UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi',
    min_compress_block_size = 1048576, max_compress_block_size = 1048576;
INSERT INTO u_zstd SELECT number, intHash64(number) FROM numbers(100000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) >= 50000
FROM (EXPLAIN SELECT count() FROM u_zstd AS a JOIN u_zstd AS b ON a.id = b.id);

SELECT 'A small table whose only part has a single granule: no estimate, one mark tells nothing about the values';
DROP TABLE IF EXISTS small;
CREATE TABLE small (id UInt64, x UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO small SELECT number, intHash64(number) FROM numbers(1000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) >= 500
FROM (EXPLAIN SELECT count() FROM small AS a JOIN small AS b ON a.id = b.id);

DROP TABLE t;
DROP TABLE u;
DROP TABLE s;
DROP TABLE u_last;
DROP TABLE u_zstd;
DROP TABLE small;
