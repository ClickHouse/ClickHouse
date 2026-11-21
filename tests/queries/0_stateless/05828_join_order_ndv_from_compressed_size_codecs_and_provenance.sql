-- The estimate of the number of distinct values from the compressed size of a column (`estimate_distinct_values_from_metadata`):
-- it takes the effective codec of a column without one (the default codec of the table), it is not made for adaptive
-- codecs and columns of the sorting key, and a key with this estimate keeps its provenance through joins and aggregations,
-- also in the Cascades optimizer.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_zstd;
DROP TABLE IF EXISTS t_adaptive;
DROP TABLE IF EXISTS t_table_delta;
DROP TABLE IF EXISTS t_column_delta;
DROP TABLE IF EXISTS t_sorted;
DROP TABLE IF EXISTS facts;
DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS measured;

-- `k` has 16 equally frequent values in random order, compressed by the default codec of the table.
CREATE TABLE t_zstd (id UInt32 CODEC(LZ4), k UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, default_compression_codec = 'ZSTD(1)', enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_zstd SELECT number, intHash32(number) % 16 FROM numbers(100000);

-- The same with adaptive codec selection, which can compress the column by a specialized codec.
CREATE TABLE t_adaptive (id UInt32 CODEC(LZ4), k UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, default_compression_codec = 'ZSTD(1)', enable_adaptive_codec_selection = 1,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_adaptive SELECT * FROM t_zstd;

-- A unique key in the order of the rows: `Delta` compresses it to almost nothing, by the order of the values. Once as the
-- default codec of the table, once as the codec of the column.
CREATE TABLE t_table_delta (id UInt32 CODEC(LZ4), k UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, default_compression_codec = 'Delta(4), ZSTD(1)', enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_table_delta SELECT number, number FROM numbers(100000);

CREATE TABLE t_column_delta (id UInt32 CODEC(LZ4), k UInt32 CODEC(Delta(4), ZSTD(1)))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, default_compression_codec = 'ZSTD(1)', enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_column_delta SELECT * FROM t_table_delta;

-- 5000 values, each repeated 20 times in the order of the sorting key: the runs compress to less than a bit per row.
CREATE TABLE t_sorted (k UInt32 CODEC(ZSTD(1)), id UInt32 CODEC(LZ4))
ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_sorted SELECT number % 5000, number FROM numbers(100000);

SELECT 'Self-joins on `k`: 1 when the selectivity is estimated from the compressed size, 0 when the join is taken as a join with a unique key.';
SELECT 'table default ZSTD', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_zstd AS a JOIN t_zstd AS b ON a.k = b.k);
SELECT 'adaptive codec', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_adaptive AS a JOIN t_adaptive AS b ON a.k = b.k);
SELECT 'table default Delta', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_table_delta AS a JOIN t_table_delta AS b ON a.k = b.k);
SELECT 'column Delta', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_column_delta AS a JOIN t_column_delta AS b ON a.k = b.k);
SELECT 'sorting key', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_sorted AS a JOIN t_sorted AS b ON a.k = b.k);

-- `fk` is a foreign key into `dim`: 90% of the rows have the key 0, so it compresses well and gets a small estimate.
CREATE TABLE facts (id UInt32 CODEC(LZ4), fk UInt32 CODEC(LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO facts SELECT number, if(number % 10 = 0, intHash32(number) % 100000, 0) FROM numbers(100000);

-- Dimension tables with a unique key that does not compress: without statistics, and with `uniq` statistics.
CREATE TABLE dim (key UInt32 CODEC(LZ4), name String)
ENGINE = MergeTree ORDER BY name
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO dim SELECT number, toString(intHash64(number)) FROM numbers(100000);

CREATE TABLE measured (key UInt32 CODEC(LZ4), name String)
ENGINE = MergeTree ORDER BY name
SETTINGS auto_statistics_types = 'uniq', min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO measured SELECT number, toString(intHash64(number)) FROM numbers(100000);

SELECT 'The key of a nested join takes the estimate of the foreign key: the outer join stays as large as the foreign key side.';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Output rows: estimated ([^\n]+)') LIKE '% thousand'
FROM
(
    EXPLAIN SELECT count()
    FROM (SELECT d.key AS key FROM facts AS f JOIN measured AS d ON f.fk = d.key LIMIT 100001) AS j
    JOIN dim AS c ON j.key = c.key
);

SELECT 'The same with the Cascades optimizer: the join is not estimated larger than its inputs.';
SET enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_workers_num = 4, explain_query_plan_default = 'legacy';
SELECT toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'JoinLogical[^\n]*\\(rows: ~([0-9.e+]+)')) BETWEEN 1 AND 200000
FROM
(
    EXPLAIN estimates = 1 SELECT count() FROM facts AS f JOIN dim AS d ON f.fk = d.key
);

SET enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_zstd;
DROP TABLE t_adaptive;
DROP TABLE t_table_delta;
DROP TABLE t_column_delta;
DROP TABLE t_sorted;
DROP TABLE facts;
DROP TABLE dim;
DROP TABLE measured;
