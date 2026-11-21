-- Tags: no-parallel-replicas
-- no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.
-- A subquery of a filter injected by `additional_table_filters` is planned under its own `SETTINGS`, the
-- same as a subquery of the query itself, so a clause that changes a setting there prevents caching with
-- `query_cache_use_only_when_data_was_not_changed = 1` too. A reset with `SETTINGS name = DEFAULT` counts
-- when it changes the value of the probing context.

DROP TABLE IF EXISTS src_05621;
DROP TABLE IF EXISTS flt_05621;
CREATE TABLE src_05621 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE flt_05621 (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO src_05621 VALUES (1), (2);
INSERT INTO flt_05621 VALUES (1), (2);

SELECT 'filter different', count() FROM {CLICKHOUSE_DATABASE:Identifier}.src_05621
SETTINGS prefer_localhost_replica = 1,
    additional_table_filters = {'src_05621': 'x IN (SELECT v FROM flt_05621 SETTINGS prefer_localhost_replica = 0)'},
    use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
    query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT 'filter same', count() FROM {CLICKHOUSE_DATABASE:Identifier}.src_05621
SETTINGS prefer_localhost_replica = 1,
    additional_table_filters = {'src_05621': 'x IN (SELECT v FROM flt_05621 SETTINGS prefer_localhost_replica = 1)'},
    use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
    query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT 'reset different', count() FROM (SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.src_05621 SETTINGS prefer_localhost_replica = DEFAULT)
SETTINGS prefer_localhost_replica = 0, use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
    query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT 'reset same', count() FROM (SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.src_05621 SETTINGS prefer_localhost_replica = DEFAULT)
SETTINGS prefer_localhost_replica = 1, use_query_cache = 1, query_cache_use_only_when_data_was_not_changed = 1,
    query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

SELECT splitByChar(',', query)[1] AS q FROM system.query_cache WHERE query LIKE '%' || currentDatabase() || '.src_05621%' ORDER BY q;

DROP TABLE src_05621;
DROP TABLE flt_05621;
