-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The replicas are not given the initiator's current database, so a read that names its tables
-- explicitly must not depend on that database existing on them. All replicas here are the same server,
-- so the current database is made one that exists nowhere by dropping it after `USE`.

SET allow_experimental_time_series_table = 1;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 0;
SET automatic_parallel_replicas_mode = 0;

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt SELECT number FROM numbers(100);

CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES ('m1', {'job': 'j1'}, [(1, 1.), (2, 2.)]), ('m2', {'job': 'j2'}, [(1, 3.)]);

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT '-- a table';
SELECT sum(x) FROM {CLICKHOUSE_DATABASE:Identifier}.mt;
-- The custom-key reads are wrapped in a subquery so the initiator merges the per-replica results.
SELECT sum(x) FROM (SELECT x FROM {CLICKHOUSE_DATABASE:Identifier}.mt)
    SETTINGS enable_parallel_replicas = 1, serialize_query_plan = 0,
             parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'x';

SELECT '-- a table function';
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM (SELECT * FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts))
    SETTINGS optimize_trivial_count_query = 0, enable_parallel_replicas = 1, serialize_query_plan = 0,
             parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'cityHash64(id)';

SELECT '-- table functions on the right side of a JOIN';
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.mt AS l
    INNER JOIN timeSeriesSelector({CLICKHOUSE_DATABASE:Identifier}.ts, 'm1', toDateTime64(0, 3), toDateTime64(10, 3)) AS r
    ON l.x = toUInt64(r.value);
