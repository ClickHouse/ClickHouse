-- The `TTL` of the engine of an inner table of a `TimeSeries` table gets the time zone of the session when the query gives it,
-- in the definition of the `TimeSeries` table and in the inner table, also in a `Replicated` database, as in
-- `05481_ttl_datetime_literal_session_timezone_materialized_view`. A `TTL` that `CREATE TABLE ... AS` copies from the source
-- keeps the text, and so the meaning, it has there.

SET allow_experimental_time_series_table = 1;
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS ts_source;
DROP TABLE IF EXISTS ts_copy;
DROP TABLE IF EXISTS ts_own;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

SET session_timezone = '';
CREATE TABLE ts_source ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree PRIMARY KEY id ORDER BY (id, bucket)
TTL toDateTime(bucket) + INTERVAL 1 SECOND DELETE WHERE bucket = '2020-01-01 00:00:00';

SET session_timezone = 'Asia/Tokyo';
CREATE TABLE ts_copy AS ts_source;
CREATE TABLE ts_own ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree PRIMARY KEY id ORDER BY (id, bucket)
TTL toDateTime(bucket) + INTERVAL 1 SECOND DELETE WHERE bucket = '2020-01-01 00:00:00';

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/05482/' || currentDatabase(), 's1', 'r1');
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_own ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree PRIMARY KEY id ORDER BY (id, bucket)
TTL toDateTime(bucket) + INTERVAL 1 SECOND DELETE WHERE bucket = '2020-01-01 00:00:00';

-- The condition of the `TTL` in the definition of the `TimeSeries` table and in its inner table of samples.
SELECT ts.database = currentDatabase() ? ts.name : 'Replicated ' || ts.name,
    extract(ts.create_table_query, 'WHERE bucket = (toDateTime64\\([^)]*\\)|\'[^\']*\')'),
    extract(inner.engine_full, 'WHERE bucket = (toDateTime64\\([^)]*\\)|\'[^\']*\')')
FROM system.tables AS ts
INNER JOIN system.tables AS inner
    ON inner.database = ts.database
    AND (inner.name = '.inner_id.samples.' || toString(ts.uuid) OR inner.name = '.inner.samples.' || ts.name)
WHERE ts.database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String}) AND ts.engine = 'TimeSeries'
ORDER BY ts.database = currentDatabase() DESC, ts.name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE ts_own;
DROP TABLE ts_copy;
DROP TABLE ts_source;
