-- The layer filter of `PartsSplitter` refers to the key columns by name and is composed with the table's
-- own key expression. A computed key suffix that is not read by the join-by-shards layers, which split only on
-- the common prefix, used to break it. An aliased key is covered by 05427_parts_splitter_layer_filter_aliased_key.

DROP TABLE IF EXISTS t_jbs_l;
DROP TABLE IF EXISTS t_jbs_r;

CREATE TABLE t_jbs_l (a UInt64, b DateTime) ENGINE = MergeTree ORDER BY (a, toDate(b)) SETTINGS index_granularity = 1;
CREATE TABLE t_jbs_r (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_jbs_l;
SYSTEM STOP MERGES t_jbs_r;
INSERT INTO t_jbs_l SELECT number, toDateTime('2020-01-01 00:00:00') + number * 3600 FROM numbers(50);
INSERT INTO t_jbs_l SELECT number, toDateTime('2020-01-01 00:00:00') + number * 3600 FROM numbers(50, 50);
INSERT INTO t_jbs_r SELECT number FROM numbers(50);
INSERT INTO t_jbs_r SELECT number FROM numbers(50, 50);

SELECT 'sharded join, unread key suffix';
SELECT count(), sum(a) FROM t_jbs_l JOIN t_jbs_r USING (a)
SETTINGS max_threads = 4, join_algorithm = 'hash', query_plan_join_shard_by_pk_ranges = 1,
         enable_join_runtime_filters = 0, max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0;

DROP TABLE t_jbs_l;
DROP TABLE t_jbs_r;
