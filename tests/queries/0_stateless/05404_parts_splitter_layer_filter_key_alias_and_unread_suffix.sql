-- The layer filter of `PartsSplitter` refers to the key columns by name and is composed with the table's
-- own key expression. Two shapes of key used to break it:
-- an aliased key, whose `sample_block` names differ from the alias-free key column names, and
-- a computed key suffix that is not read by the join-by-shards layers, which split only on the common prefix.

DROP TABLE IF EXISTS t_pk_alias;
DROP TABLE IF EXISTS t_jbs_l;
DROP TABLE IF EXISTS t_jbs_r;

CREATE TABLE t_pk_alias (k UInt64, v UInt64) ENGINE = ReplacingMergeTree PRIMARY KEY (k AS a) ORDER BY k SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_pk_alias;
INSERT INTO t_pk_alias VALUES (1, 1), (3, 1);
INSERT INTO t_pk_alias VALUES (2, 2), (4, 2);
INSERT INTO t_pk_alias VALUES (2, 3), (5, 3);

SELECT 'aliased key, FINAL';
SELECT k, v FROM t_pk_alias FINAL ORDER BY k
SETTINGS max_threads = 4, do_not_merge_across_partitions_select_final = 0,
         split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;

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

DROP TABLE t_pk_alias;
DROP TABLE t_jbs_l;
DROP TABLE t_jbs_r;
