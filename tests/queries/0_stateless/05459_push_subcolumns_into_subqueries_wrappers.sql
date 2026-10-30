-- The guards of the subcolumn pushdown look through storages that forward the read to other tables
-- (`Merge`, a materialized view): the key of the underlying table keeps `length(s)` literal, and the parts
-- of the underlying table with a lazily changed `JSON` type hint keep the whole column read.
-- A secondary query of parallel replicas does not push subcolumns again, so its header matches the initiator.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS idx_src_mv;
DROP TABLE IF EXISTS lazy_hint_mv;
DROP TABLE IF EXISTS idx_src;
DROP TABLE IF EXISTS t_lazy_hint;
DROP TABLE IF EXISTS t_pr_tuple;
DROP TABLE IF EXISTS mv_feed;

CREATE TABLE mv_feed (x UInt64, s String, json JSON) ENGINE = Null;

CREATE TABLE idx_src (s String) ENGINE = MergeTree ORDER BY length(s) SETTINGS index_granularity = 128;
INSERT INTO idx_src SELECT repeat('a', number % 100) FROM numbers(100000);
CREATE MATERIALIZED VIEW idx_src_mv TO idx_src AS SELECT s FROM mv_feed;

SELECT 'primary key under Merge and a materialized view';
SELECT count() FROM (SELECT s FROM merge(currentDatabase(), '^idx_src$')) WHERE length(s) = 5 SETTINGS force_primary_key = 1;
SELECT count() FROM (SELECT s FROM (SELECT s FROM merge(currentDatabase(), '^idx_src$'))) WHERE length(s) = 5 SETTINGS force_primary_key = 1;
SELECT count() FROM (SELECT s FROM idx_src_mv) WHERE length(s) = 5 SETTINGS force_primary_key = 1;

CREATE TABLE t_lazy_hint (x UInt64, json JSON(SHARED a UInt64))
ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
ALTER TABLE t_lazy_hint MODIFY COLUMN json JSON(SHARED a UInt64, SHARED b UInt64) SETTINGS enable_json_lazy_type_hints = 1;
INSERT INTO t_lazy_hint SELECT number, concat('{"a":', toString(number), ',"b":', toString(number * 2), '}') FROM numbers(40);
ALTER TABLE t_lazy_hint MODIFY COLUMN json JSON(SHARED a UInt64) SETTINGS enable_json_lazy_type_hints = 1;
CREATE MATERIALIZED VIEW lazy_hint_mv TO t_lazy_hint AS SELECT x, json FROM mv_feed;

SELECT 'typed subcolumn under Merge and a materialized view over a part with a lazily removed type hint';
SELECT sum(json.b.:Int64) FROM (SELECT json FROM merge(currentDatabase(), '^t_lazy_hint$')) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT sum(json.b.:Int64) FROM (SELECT json FROM merge(currentDatabase(), '^t_lazy_hint$'));
SELECT sum(json.b.:Int64) FROM (SELECT json FROM lazy_hint_mv) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT sum(json.b.:Int64) FROM (SELECT json FROM lazy_hint_mv);

CREATE TABLE t_pr_tuple (k UInt64, c1 Tuple(Nullable(Int32)), arr Array(UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_tuple SELECT number, tuple(if(number = 0, 42, NULL)), [number] FROM numbers(142);

SELECT 'parallel replicas';
SELECT count(), sum(c1.1 IS NULL), sum(length(arr)) FROM (SELECT k, c1, arr FROM t_pr_tuple)
SETTINGS enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
    optimize_functions_to_subcolumns = 0;
SELECT count(), sum(c1.1 IS NULL), sum(length(arr)) FROM (SELECT k, c1, arr FROM t_pr_tuple)
SETTINGS enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
    optimize_functions_to_subcolumns = 1;

DROP TABLE idx_src_mv;
DROP TABLE lazy_hint_mv;
DROP TABLE idx_src;
DROP TABLE t_lazy_hint;
DROP TABLE t_pr_tuple;
DROP TABLE mv_feed;
