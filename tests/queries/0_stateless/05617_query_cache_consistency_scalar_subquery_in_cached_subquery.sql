-- Tags: no-parallel-replicas
-- no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.
-- A subquery cached with `query_cache_use_only_when_data_was_not_changed = 1` hashes the tables of its
-- AST, but a scalar subquery inside it is executed by the analyzer (possibly from an unvalidated cache
-- entry) and replaced by a constant before the subquery is planned. Such a subquery must not be cached.

DROP TABLE IF EXISTS src_05617;
CREATE TABLE src_05617 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src_05617 VALUES (1);

SELECT * FROM
(
    SELECT
    (
        SELECT sum(x) FROM {CLICKHOUSE_DATABASE:Identifier}.src_05617
        SETTINGS use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 0,
            query_cache_min_query_duration = 0, query_cache_min_query_runs = 0
    ) AS value
    SETTINGS use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
        query_cache_min_query_duration = 0, query_cache_min_query_runs = 0
)
SETTINGS use_query_cache = 0;

-- The subquery with the scalar subquery is not cached.
SELECT count() FROM system.query_cache WHERE query LIKE '%' || currentDatabase() || '.src_05617%' AND query LIKE '%AS value%';

INSERT INTO src_05617 VALUES (2);

SELECT * FROM
(
    SELECT
    (
        SELECT sum(x) FROM {CLICKHOUSE_DATABASE:Identifier}.src_05617
        SETTINGS use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 0,
            query_cache_min_query_duration = 0, query_cache_min_query_runs = 0
    ) AS value
    SETTINGS use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
        query_cache_min_query_duration = 0, query_cache_min_query_runs = 0
)
SETTINGS use_query_cache = 0;

DROP TABLE src_05617;
