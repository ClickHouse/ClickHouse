-- A setting written in `CREATE TABLE ... AS` overrides a setting the source repeats, whose last occurrence is the one it applies.

SET enable_time_series_table = 1;

CREATE TABLE src ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 10, recent_samples_ttl_seconds = 20;
CREATE TABLE dst AS src SETTINGS recent_samples_ttl_seconds = 0;

SELECT extractAll(create_table_query, 'recent_samples_ttl_seconds = \\d+') FROM system.tables WHERE database = currentDatabase() AND name = 'dst';

-- No recent samples target for `dst`.
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%.recentsamples.%';

-- Without an override the copy keeps the value the source applies.
CREATE TABLE dst_inherited AS src;
SELECT extractAll(create_table_query, 'recent_samples_ttl_seconds = \\d+') FROM system.tables WHERE database = currentDatabase() AND name = 'dst_inherited';
