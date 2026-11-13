-- Tags: no-random-settings, no-random-merge-tree-settings
-- The plan-level distributed read of distributed lazy materialization sends a bare `ReadFromTable` to the shards,
-- which skips the planning of the source table. Row policies, `additional_table_filters` and `ALIAS` columns
-- of the source table therefore keep the classic path (a row policy on a `MergeTree` source is not checked here:
-- https://github.com/ClickHouse/ClickHouse/issues/112891). So does `ORDER BY` without `LIMIT`, which the classic
-- path sorts on each shard. The results must match the classic path.

SET serialize_query_plan = 1;
SET query_plan_optimize_distributed_lazy_materialization = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;
SET distributed_push_down_limit = 1;
SET skip_unavailable_shards = 0;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS dlm_src_dist;
DROP TABLE IF EXISTS dlm_src;
DROP TABLE IF EXISTS dlm_alias_dist;
DROP TABLE IF EXISTS dlm_alias;
DROP TABLE IF EXISTS dlm_sparse_dist;
DROP TABLE IF EXISTS dlm_sparse;

CREATE TABLE dlm_src (x UInt64, payload String) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dlm_src_dist AS dlm_src ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dlm_src);
INSERT INTO dlm_src SELECT number, toString(number) FROM numbers(10);

SELECT '-- Without anything on the source table, the coordinated Top-K is used';
SELECT count() > 0 FROM (EXPLAIN actions = 1, distributed = 1 SELECT payload FROM dlm_src_dist ORDER BY x DESC LIMIT 3) WHERE explain LIKE '%Distributed TopK candidate limit%';
SELECT payload FROM dlm_src_dist ORDER BY x DESC LIMIT 3;

SELECT '-- ORDER BY without LIMIT keeps the classic path';
SELECT count() FROM (EXPLAIN distributed = 1 SELECT payload FROM dlm_src_dist ORDER BY x) WHERE explain LIKE '%Merge sorted streams%';
SELECT groupArray(payload) FROM (SELECT payload FROM dlm_src_dist ORDER BY x);

SELECT '-- additional_table_filters keyed by the source table';
SELECT payload FROM dlm_src_dist ORDER BY x LIMIT 4 SETTINGS additional_table_filters = {'dlm_src': 'x > 5'};

SELECT '-- ALIAS column of the source table';
CREATE TABLE dlm_alias (x UInt64, y UInt64 ALIAS x + 1) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dlm_alias_dist (y UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dlm_alias);
INSERT INTO dlm_alias SELECT number FROM numbers(10);
SELECT y FROM dlm_alias_dist ORDER BY y DESC LIMIT 3;
SELECT y FROM dlm_alias_dist ORDER BY y DESC LIMIT 3 SETTINGS serialize_query_plan = 0;

SELECT '-- Sparse sort key read in order by one stream';
CREATE TABLE dlm_sparse (key UInt64, payload String) ENGINE = MergeTree ORDER BY key
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1, min_bytes_for_wide_part = 0;
CREATE TABLE dlm_sparse_dist AS dlm_sparse ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dlm_sparse);
INSERT INTO dlm_sparse SELECT if(number < 990, 0, number), toString(number) FROM numbers(1000);
SELECT count() > 0 FROM (EXPLAIN actions = 1, distributed = 1 SELECT payload FROM dlm_sparse_dist ORDER BY key DESC LIMIT 3 SETTINGS max_threads = 1, optimize_read_in_order = 1) WHERE explain LIKE '%Distributed TopK candidate limit%';
SELECT payload FROM dlm_sparse_dist ORDER BY key DESC LIMIT 3 SETTINGS max_threads = 1, optimize_read_in_order = 1;
SELECT payload FROM dlm_sparse_dist ORDER BY key LIMIT 3 OFFSET 1980 SETTINGS max_threads = 1, optimize_read_in_order = 1;

DROP TABLE dlm_sparse_dist;
DROP TABLE dlm_sparse;
DROP TABLE dlm_alias_dist;
DROP TABLE dlm_alias;
DROP TABLE dlm_src_dist;
DROP TABLE dlm_src;
