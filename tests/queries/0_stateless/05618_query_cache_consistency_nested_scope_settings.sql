-- Tags: no-parallel-replicas
-- no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.
-- The tables of a query cached with `query_cache_use_only_when_data_was_not_changed = 1` are probed with
-- the context of the query, while the analyzer plans a subquery with its own `SETTINGS` under a separate
-- context. When such a clause changes a setting, the probe cannot follow the read (e.g. a `Distributed`
-- table read over TCP with `prefer_localhost_replica = 0`), so the query is not cached. Settings equal to
-- those of the query and the query cache settings do not prevent caching.

DROP TABLE IF EXISTS src_05618;
CREATE TABLE src_05618 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src_05618 VALUES (1), (2);

SELECT 'different', count() FROM (SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.src_05618 SETTINGS prefer_localhost_replica = 0)
SETTINGS prefer_localhost_replica = 1, use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
    query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT 'same', count() FROM (SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.src_05618 SETTINGS prefer_localhost_replica = 1)
SETTINGS prefer_localhost_replica = 1, use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
    query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT 'cache settings', count() FROM (SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.src_05618 SETTINGS query_cache_ttl = 1000)
SETTINGS use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
    query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT splitByChar(',', query)[1] AS q FROM system.query_cache WHERE query LIKE '%' || currentDatabase() || '.src_05618%' ORDER BY q;

DROP TABLE src_05618;
