-- The estimate of the number of distinct values from the compressed size of a column (`estimate_distinct_values_from_metadata`)
-- takes the codec that the parts are stored with rather than the declared one: `ALTER TABLE ... MODIFY COLUMN ... CODEC`
-- changes the declaration without rewriting the parts.
-- The local disk is set explicitly: on a remote disk the files of the parts are not read to find their codec.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_delta_to_zstd;
DROP TABLE IF EXISTS t_delta_to_default;
DROP TABLE IF EXISTS t_zstd_to_lz4;

-- A unique key in the order of the rows: `Delta` compresses it to almost nothing, by the order of the values.
CREATE TABLE t_delta_to_zstd (id UInt32 CODEC(LZ4), k UInt32 CODEC(Delta(4), ZSTD(1)))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', auto_statistics_types = '', min_bytes_for_wide_part = 0, default_compression_codec = 'ZSTD(1)', enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_delta_to_zstd SELECT number, number FROM numbers(100000);

CREATE TABLE t_delta_to_default AS t_delta_to_zstd;
INSERT INTO t_delta_to_default SELECT * FROM t_delta_to_zstd;

-- 16 equally frequent values in random order, compressed by a general purpose codec.
CREATE TABLE t_zstd_to_lz4 (id UInt32 CODEC(LZ4), k UInt32 CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', auto_statistics_types = '', min_bytes_for_wide_part = 0, default_compression_codec = 'ZSTD(1)', enable_adaptive_codec_selection = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO t_zstd_to_lz4 SELECT number, intHash32(number) % 16 FROM numbers(100000);

-- The parts stay as they were written.
ALTER TABLE t_delta_to_zstd MODIFY COLUMN k CODEC(ZSTD(1));
ALTER TABLE t_delta_to_default MODIFY COLUMN k REMOVE CODEC;
ALTER TABLE t_zstd_to_lz4 MODIFY COLUMN k CODEC(LZ4);

SELECT 'Self-joins on `k`: 1 when the selectivity is estimated from the compressed size, 0 when the join is taken as a join with a unique key.';
SELECT 'Delta altered to ZSTD', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_delta_to_zstd AS a JOIN t_delta_to_zstd AS b ON a.k = b.k);
SELECT 'Delta altered to the default codec', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_delta_to_default AS a JOIN t_delta_to_default AS b ON a.k = b.k);
SELECT 'ZSTD altered to LZ4', toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 1 / 1024
FROM (EXPLAIN SELECT count() FROM t_zstd_to_lz4 AS a JOIN t_zstd_to_lz4 AS b ON a.k = b.k);

DROP TABLE t_delta_to_zstd;
DROP TABLE t_delta_to_default;
DROP TABLE t_zstd_to_lz4;
