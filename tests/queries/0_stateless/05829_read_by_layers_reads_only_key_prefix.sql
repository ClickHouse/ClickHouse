-- A read in order split into layers by a prefix of the sorting key reads only the columns of that prefix and of the query,
-- not the other columns of the sorting key.

DROP TABLE IF EXISTS t_layers_prefix;
-- Without statistics: a query that loads them counts their compressed bytes in `ReadCompressedBytes` too.
CREATE TABLE t_layers_prefix (k UInt32 CODEC(Delta, LZ4), s String) ENGINE = MergeTree ORDER BY (k, s)
SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0, auto_statistics_types = '';

SYSTEM STOP MERGES t_layers_prefix;
INSERT INTO t_layers_prefix SELECT number, hex(sipHash128(toString(number))) FROM numbers(100000);
INSERT INTO t_layers_prefix SELECT number, hex(sipHash128(toString(number + 1000000))) FROM numbers(100000);

-- Without the caches that skip the read of compressed data, which `ReadCompressedBytes` counts.
SET max_threads = 4, enable_parallel_replicas = 0, use_uncompressed_cache = 0, use_columns_cache = 0;

-- A single `uniqExact` of the first sorting key column, counted in order.
SET optimize_aggregation_in_order = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count(DISTINCT k) FROM t_layers_prefix) WHERE explain LIKE '%FilterSortedStreamByRange%';
SELECT count(DISTINCT k) FROM t_layers_prefix SETTINGS log_comment = '05829_uniq_exact';

-- A merge join of reads in order, joined by ranges of the primary key.
SET join_algorithm = 'full_sorting_merge', query_plan_join_shard_by_pk_ranges = 1, optimize_read_in_order = 1;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() FROM t_layers_prefix AS l JOIN t_layers_prefix AS r ON l.k = r.k) WHERE explain LIKE '%FilterSortedStreamByRange%';
SELECT count() FROM t_layers_prefix AS l JOIN t_layers_prefix AS r ON l.k = r.k SETTINGS log_comment = '05829_join';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ReadCompressedBytes'] < (SELECT data_compressed_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_layers_prefix' AND name = 's') / 10
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05829_uniq_exact', '05829_join') AND type = 'QueryFinish' AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE t_layers_prefix;

-- The sorting key keeps its own names when the query settings would name its expressions differently: `IN` is `nullIn`
-- with `transform_null_in`. The first key column is fixed by the condition, so the read is in order of `k`, in layers
-- with several threads and by merging the parts with one.
DROP TABLE IF EXISTS t_layers_prefix_in;
CREATE TABLE t_layers_prefix_in (a UInt32, k UInt32, s String) ENGINE = MergeTree ORDER BY (a IN (1, 2), k, s)
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_layers_prefix_in;
INSERT INTO t_layers_prefix_in SELECT 1, number % 500, toString(number) FROM numbers(10000);
INSERT INTO t_layers_prefix_in SELECT 1, number % 500, toString(number) FROM numbers(10000);

SET transform_null_in = 1, read_in_order_two_level_merge_threshold = 1;
SELECT uniqExact(k) FROM t_layers_prefix_in WHERE a = 1 SETTINGS max_threads = 4;
SELECT uniqExact(k) FROM t_layers_prefix_in WHERE a = 1 SETTINGS max_threads = 1;
SELECT k, count() FROM t_layers_prefix_in WHERE a = 1 GROUP BY k ORDER BY k LIMIT 1 SETTINGS max_threads = 1;
SELECT k FROM t_layers_prefix_in WHERE a = 1 ORDER BY k LIMIT 1 SETTINGS max_threads = 1;

DROP TABLE t_layers_prefix_in;
