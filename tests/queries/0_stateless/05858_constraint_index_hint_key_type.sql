-- an index hint compares the key with the constant of the query: the result must not change when the key has another type or time zone

SET convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS t_hint_datetime;
DROP TABLE IF EXISTS t_hint_datetime64;
DROP TABLE IF EXISTS t_hint_ip;
DROP TABLE IF EXISTS t_hint_time_zone;

CREATE TABLE t_hint_datetime (n UInt32, dt DateTime('UTC'), CONSTRAINT c CHECK n = dt) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_hint_datetime VALUES (1577880000, '2020-01-01 12:00:00');
SELECT 'datetime', n FROM t_hint_datetime WHERE dt < toDate('2020-01-02') SETTINGS optimize_append_index = 0;
SELECT 'datetime', n FROM t_hint_datetime WHERE dt < toDate('2020-01-02') SETTINGS optimize_append_index = 1;

CREATE TABLE t_hint_datetime64 (n Int64, dt DateTime64(3, 'UTC'), CONSTRAINT c CHECK n = dt) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_hint_datetime64 VALUES (1577880000, '2020-01-01 12:00:00');
SELECT 'datetime64', n FROM t_hint_datetime64 WHERE dt < toDate('2020-01-02') SETTINGS optimize_append_index = 0;
SELECT 'datetime64', n FROM t_hint_datetime64 WHERE dt < toDate('2020-01-02') SETTINGS optimize_append_index = 1;

CREATE TABLE t_hint_ip (n UInt32, ip IPv4, CONSTRAINT c CHECK n = ip) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_hint_ip VALUES (2130706433, '127.0.0.1');
SELECT 'ip', n FROM t_hint_ip WHERE ip = toIPv6('::ffff:127.0.0.1') SETTINGS optimize_append_index = 0;
SELECT 'ip', n FROM t_hint_ip WHERE ip = toIPv6('::ffff:127.0.0.1') SETTINGS optimize_append_index = 1;

-- a date or a string is a time in the time zone of the column
CREATE TABLE t_hint_time_zone (k DateTime('Asia/Tokyo'), dt DateTime('UTC'), CONSTRAINT c CHECK k = dt) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_hint_time_zone VALUES (toDateTime('2020-01-01 20:00:00', 'UTC'), toDateTime('2020-01-01 20:00:00', 'UTC'));
SELECT 'time zone date', dt FROM t_hint_time_zone WHERE dt < toDate('2020-01-02') SETTINGS optimize_append_index = 0;
SELECT 'time zone date', dt FROM t_hint_time_zone WHERE dt < toDate('2020-01-02') SETTINGS optimize_append_index = 1;
SELECT 'time zone string', dt FROM t_hint_time_zone WHERE dt < '2020-01-02 00:00:00' SETTINGS optimize_append_index = 0;
SELECT 'time zone string', dt FROM t_hint_time_zone WHERE dt < '2020-01-02 00:00:00' SETTINGS optimize_append_index = 1;

DROP TABLE t_hint_datetime;
DROP TABLE t_hint_datetime64;
DROP TABLE t_hint_ip;
DROP TABLE t_hint_time_zone;
