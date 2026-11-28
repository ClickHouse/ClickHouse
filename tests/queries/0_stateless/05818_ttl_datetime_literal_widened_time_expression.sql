-- The expression of a `TTL`, which merges evaluate to get the time, is analyzed with `DateTime` columns widened to `DateTime64(0)`,
-- so a literal compared with such a column there is read as a `DateTime64(0)`, also before 1970. With `session_timezone`, the
-- literal is stored with the session time zone as a `DateTime64(0)` too, rather than as a `DateTime`, which would clamp
-- `1960-01-01` to the start of 1970 and make the `TTL` delete the row stored at that time.

SET session_timezone = 'UTC';

SELECT '--- CREATE';
DROP TABLE IF EXISTS t_ttl_widened;
CREATE TABLE t_ttl_widened (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id
TTL if(event = '1960-01-01 00:00:00', toDateTime('2000-01-01 00:00:00'), toDateTime('2100-01-01 00:00:00'));
SELECT replaceRegexpOne(extract(create_table_query, 'TTL .*'), ' SETTINGS .*', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_widened';
INSERT INTO t_ttl_widened VALUES (1, 0), (2, 1);
OPTIMIZE TABLE t_ttl_widened FINAL SETTINGS session_timezone = '';
SELECT id FROM t_ttl_widened ORDER BY id;
DROP TABLE t_ttl_widened;

SELECT '--- MODIFY TTL';
CREATE TABLE t_ttl_widened (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_ttl_widened MODIFY TTL if(event = '1960-01-01 00:00:00', toDateTime('2000-01-01 00:00:00'), toDateTime('2100-01-01 00:00:00'));
SELECT replaceRegexpOne(extract(create_table_query, 'TTL .*'), ' SETTINGS .*', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_widened';
INSERT INTO t_ttl_widened VALUES (1, 0), (2, 1);
OPTIMIZE TABLE t_ttl_widened FINAL SETTINGS session_timezone = '';
SELECT id FROM t_ttl_widened ORDER BY id;
DROP TABLE t_ttl_widened;

SELECT '--- a DateTime element of a Tuple column';
CREATE TABLE t_ttl_widened (id UInt8, n Tuple(ts DateTime)) ENGINE = MergeTree ORDER BY id
TTL if(n.ts = '1960-01-01 00:00:00', toDateTime('2000-01-01 00:00:00'), toDateTime('2100-01-01 00:00:00'));
SELECT replaceRegexpOne(extract(create_table_query, 'TTL .*'), ' SETTINGS .*', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_widened';
INSERT INTO t_ttl_widened VALUES (1, tuple(0)), (2, tuple(1));
OPTIMIZE TABLE t_ttl_widened FINAL SETTINGS session_timezone = '';
SELECT id FROM t_ttl_widened ORDER BY id;
DROP TABLE t_ttl_widened;

SELECT '--- DELETE WHERE, which is analyzed with the original columns';
CREATE TABLE t_ttl_widened (id UInt8, event DateTime) ENGINE = MergeTree ORDER BY id
TTL event + INTERVAL 1 DAY DELETE WHERE event = '2020-01-01 00:00:00';
SELECT replaceRegexpOne(extract(create_table_query, 'TTL .*'), ' SETTINGS .*', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_widened';
DROP TABLE t_ttl_widened;
