-- Tags: zookeeper, no-replicated-database, no-parallel-replicas
-- Tag no-replicated-database: the replicated tables have their own Keeper paths, and the test creates a Replicated database.
-- Tag no-parallel-replicas: the replicas execute the `timeSeriesMetricFamilies` table function without coordinating
-- with the initiator, so a `count()` answered from the primary key index alone is summed up once per replica,
-- see https://github.com/ClickHouse/ClickHouse/issues/118130.

SET allow_experimental_time_series_table = 1;
SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

SELECT '--- replicated inner tables outside of a Replicated database: TRUNCATE on one replica, the other replica writes the tags and the metric family again ---';
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
-- Two replicas of the inner tags and metric families tables in one server. The inner tables don't deduplicate
-- the inserted blocks, so only the caches of the `TimeSeries` table can skip the rows written before.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_r1 ENGINE = TimeSeries
    TAGS INNER ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05843/tags', 'r1') ORDER BY (metric_name, id) SETTINGS replicated_deduplication_window = 0
    METRIC FAMILIES INNER ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05843/metric_families', 'r1') ORDER BY metric_family SETTINGS replicated_deduplication_window = 0;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_r2 ENGINE = TimeSeries
    TAGS INNER ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05843/tags', 'r2') ORDER BY (metric_name, id) SETTINGS replicated_deduplication_window = 0
    METRIC FAMILIES INNER ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05843/metric_families', 'r2') ORDER BY metric_family SETTINGS replicated_deduplication_window = 0;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts_r2 (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts_r2 (metric_family, type, unit, help) VALUES ('http_requests', 'counter', '', 'first');
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);

TRUNCATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_r1 SETTINGS replication_alter_partitions_sync = 2;
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts_r2 (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts_r2 (metric_family, type, unit, help) VALUES ('http_requests', 'counter', '', 'first');
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);
-- The inner samples table of `ts_r2` is not replicated, so both samples are there and found through the tags written again.
SELECT sum(length(samples)) FROM timeSeriesSelector({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2, 'http_requests', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:01:00', 3), 'samples');

SELECT '--- in a Replicated database TRUNCATE is executed on every replica, so the caches are used with replicated inner tables ---';
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Replicated('/clickhouse/databases/{database}/05843', 's1', 'r1');
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.ts ENGINE = TimeSeries
    TAGS INNER ENGINE = ReplicatedMergeTree ORDER BY (metric_name, id) SETTINGS replicated_deduplication_window = 0
    METRIC FAMILIES INNER ENGINE = ReplicatedMergeTree ORDER BY metric_family SETTINGS replicated_deduplication_window = 0;

INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.ts (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.ts (metric_family, type, unit, help) VALUES ('http_requests', 'counter', '', 'first');
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.ts (metric_family, type, unit, help) VALUES ('http_requests', 'counter', '', 'first');
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE_2:Identifier}.ts);
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE_2:Identifier}.ts);

SELECT '--- outside of a Replicated database the same inserts write their rows every time ---';
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts_r2 (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts_r2 (metric_family, type, unit, help) VALUES ('http_requests', 'counter', '', 'first');
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);
SELECT count() FROM timeSeriesMetricFamilies({CLICKHOUSE_DATABASE_1:Identifier}.ts_r2);

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} SYNC;
