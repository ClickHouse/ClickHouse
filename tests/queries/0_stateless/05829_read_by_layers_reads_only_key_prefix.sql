-- A read in order split into layers by a prefix of the sorting key reads only the columns of that prefix and of the query,
-- not the other columns of the sorting key.

DROP TABLE IF EXISTS t_layers_prefix;
CREATE TABLE t_layers_prefix (k UInt32, s String) ENGINE = MergeTree ORDER BY (k, s)
SETTINGS index_granularity = 1024, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_layers_prefix;
INSERT INTO t_layers_prefix SELECT intDiv(number, 100), repeat(toString(cityHash64(number)), 10) FROM numbers(500000);
INSERT INTO t_layers_prefix SELECT intDiv(number, 100), repeat(toString(cityHash64(number + 1)), 10) FROM numbers(500000);

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
