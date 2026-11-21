-- `CREATE TABLE ... AS <time_series>` under `uuid_type_version = 2` keeps the historical `UUID` semantics of the
-- `id_generator` setting copied from the old table, while a generator written in the query itself is materialized.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

-- The `id_type` and `id_generator` settings apply to tables of version 9 or earlier only.

SET allow_experimental_time_series_table = 1;

SET uuid_type_version = 1;
CREATE TABLE ts_old ENGINE = TimeSeries SETTINGS version = 9, id_type = 'UInt64',
    id_generator = 'plus(sipHash64(metric_name, all_tags), CAST(\'00000000-0000-0001-0000-000000000000\', \'UUID\') < CAST(\'00000000-0000-0000-0000-000000000002\', \'UUID\'))';

SET uuid_type_version = 2;
CREATE TABLE ts_copy AS ts_old ENGINE = TimeSeries SETTINGS version = 9;
CREATE TABLE ts_new ENGINE = TimeSeries SETTINGS version = 9, id_type = 'UInt64',
    id_generator = 'plus(sipHash64(metric_name, all_tags), CAST(\'00000000-0000-0001-0000-000000000000\', \'UUID\') < CAST(\'00000000-0000-0000-0000-000000000002\', \'UUID\'))';

SELECT name, extract(create_table_query, 'id_generator = (\'(?:[^\'\\\\]|\\\\.)*\')')
FROM system.tables WHERE database = currentDatabase() AND name LIKE 'ts_%' ORDER BY name FORMAT TSVRaw;

-- The copied table computes the same identifiers as the old one.
INSERT INTO ts_old (metric_name, tags, samples) VALUES ('http_requests', map('job', 'api'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.0)]);
INSERT INTO ts_copy (metric_name, tags, samples) VALUES ('http_requests', map('job', 'api'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.0)]);
SELECT (SELECT groupArray(id) FROM timeSeriesTags(ts_old)) = (SELECT groupArray(id) FROM timeSeriesTags(ts_copy));

DROP TABLE ts_old;
DROP TABLE ts_copy;
DROP TABLE ts_new;
