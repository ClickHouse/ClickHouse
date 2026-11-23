-- Shuffled aggregation-in-order on skewed inputs. The per-shard sorted merges consume their inputs selectively,
-- so the repartitioning relies on progress markers (virtual rows) instead of buffering: it must neither hang nor
-- read ahead without limit, whatever the distribution of the keys, and must return the same result as the
-- ordinary aggregation.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;
-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514).
SET max_rows_to_group_by = 0;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE IF EXISTS t_aio_shuffle_skew;
CREATE TABLE t_aio_shuffle_skew (k UInt64, v UInt64, s String, lc LowCardinality(String), d Dynamic)
ENGINE = MergeTree ORDER BY k
-- The query under `max_memory_usage` below holds up to 16 blocks per stream plus the read buffers, so its memory
-- depends on the size of a block and of a compressed block. Do not let the randomized settings enlarge them.
SETTINGS index_granularity = 8192, index_granularity_bytes = '10M', min_compress_block_size = 65536, max_compress_block_size = 1048576;

-- Every part holds a single key, so each input stream sends all of its rows to one shard for a long time, while
-- every other shard waits for that stream. Without markers this either deadlocks or buffers whole parts.
SYSTEM STOP MERGES t_aio_shuffle_skew;
INSERT INTO t_aio_shuffle_skew SELECT 1, number, toString(number), toString(number % 100), number FROM numbers(1000000);
INSERT INTO t_aio_shuffle_skew SELECT 2, number, toString(number), toString(number % 100), number FROM numbers(1000000);
INSERT INTO t_aio_shuffle_skew SELECT 3, number, toString(number), toString(number % 100), number FROM numbers(1000000);
INSERT INTO t_aio_shuffle_skew SELECT 4, number, toString(number), toString(number % 100), number FROM numbers(1000000);
INSERT INTO t_aio_shuffle_skew SELECT 5, number, toString(number), toString(number % 100), number FROM numbers(1000000);
INSERT INTO t_aio_shuffle_skew SELECT 6, number, toString(number), toString(number % 100), number FROM numbers(1000000);
INSERT INTO t_aio_shuffle_skew SELECT 7, number, toString(number), toString(number % 100), number FROM numbers(1000000);
INSERT INTO t_aio_shuffle_skew SELECT 8, number, toString(number), toString(number % 100), number FROM numbers(1000000);

SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle_skew GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The repartitioning holds a few blocks per stream, not whole parts: it takes under 20 MB with blocks of 8192 rows,
-- while a part is about 50 MB in memory, and seven of the eight streams wait for the first one.
SELECT k, sum(v), max(s), max(lc), count(d) FROM t_aio_shuffle_skew GROUP BY k ORDER BY k
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1, max_block_size = 8192,
    max_memory_usage = '100M';

-- `GROUP BY k, v % 7` reads in the order of `k` alone, so rows with equal sort keys go to different shards.
SELECT
    (SELECT groupBitXor(cityHash64(k, m, c, x)) FROM (SELECT k, v % 7 AS m, count() AS c, max(s) AS x FROM t_aio_shuffle_skew GROUP BY k, m)
        SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(k, m, c, x)) FROM (SELECT k, v % 7 AS m, count() AS c, max(s) AS x FROM t_aio_shuffle_skew GROUP BY k, m)
        SETTINGS max_threads = 8, optimize_aggregation_in_order = 0);

-- Runs of keys that interleave between parts, with many more shards than streams.
TRUNCATE TABLE t_aio_shuffle_skew;
INSERT INTO t_aio_shuffle_skew SELECT intDiv(number, 50000) * 3, number, '', '', NULL FROM numbers(600000);
INSERT INTO t_aio_shuffle_skew SELECT intDiv(number, 70000) * 3 + 1, number, '', '', NULL FROM numbers(600000);
INSERT INTO t_aio_shuffle_skew SELECT intDiv(number, 30000) * 2, number, '', '', NULL FROM numbers(600000);

SELECT
    (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) AS s FROM t_aio_shuffle_skew GROUP BY k)
        SETTINGS max_threads = 16, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1)
  = (SELECT groupBitXor(cityHash64(k, s)) FROM (SELECT k, sum(v) AS s FROM t_aio_shuffle_skew GROUP BY k)
        SETTINGS max_threads = 16, optimize_aggregation_in_order = 0);

DROP TABLE t_aio_shuffle_skew;

-- Aggregate states that keep their payload outside of arenas are shuffled as well.
DROP TABLE IF EXISTS t_aio_shuffle_skew_states;
CREATE TABLE t_aio_shuffle_skew_states (k UInt64, u AggregateFunction(uniqExact, UInt64), a AggregateFunction(groupArray, UInt64))
ENGINE = AggregatingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_aio_shuffle_skew_states;
INSERT INTO t_aio_shuffle_skew_states SELECT number % 4, uniqExactState(number), groupArrayState(number) FROM numbers(10000) GROUP BY number % 4;
INSERT INTO t_aio_shuffle_skew_states SELECT number % 4, uniqExactState(number), groupArrayState(number) FROM numbers(10000, 10000) GROUP BY number % 4;

SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, uniqExactMerge(u) FROM t_aio_shuffle_skew_states GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

SELECT k, uniqExactMerge(u), length(groupArrayMerge(a)) FROM t_aio_shuffle_skew_states GROUP BY k ORDER BY k
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1;

DROP TABLE t_aio_shuffle_skew_states;
