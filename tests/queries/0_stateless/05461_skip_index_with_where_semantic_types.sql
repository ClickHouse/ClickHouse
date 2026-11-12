-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- A partial index is used only if the query calculates its condition with the same types as the writer did: `Bool` is not `UInt8`,
-- and a `DateTime` or `DateTime64` with another time zone formats a different string.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_datetime;
DROP TABLE IF EXISTS t_datetime64;

CREATE TABLE t_bool (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE toString(if(p, true, false)) = '1') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bool VALUES (1, 1);
CREATE TABLE t_datetime (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE toString(if(p, toDateTime(0, 'UTC'), toDateTime(0, 'UTC'))) = '1970-01-01 09:00:00') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_datetime VALUES (1, 1);
CREATE TABLE t_datetime64 (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE toString(if(p, toDateTime64(0, 3, 'UTC'), toDateTime64(0, 3, 'UTC'))) = '1970-01-01 09:00:00.000') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_datetime64 VALUES (1, 1);

SELECT 'Bool and UInt8';
SELECT count() FROM t_bool WHERE id = 1 AND toString(if(p, toUInt8(1), toUInt8(0))) = '1' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bool WHERE id = 1 AND toString(if(p, toUInt8(1), toUInt8(0))) = '1' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_bool WHERE id = 1 AND toString(if(p, toUInt8(1), toUInt8(0))) = '1' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'DateTime time zones';
SELECT count() FROM t_datetime WHERE id = 1 AND toString(if(p, toDateTime(0, 'Asia/Tokyo'), toDateTime(0, 'Asia/Tokyo'))) = '1970-01-01 09:00:00' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_datetime WHERE id = 1 AND toString(if(p, toDateTime(0, 'Asia/Tokyo'), toDateTime(0, 'Asia/Tokyo'))) = '1970-01-01 09:00:00' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_datetime WHERE id = 1 AND toString(if(p, toDateTime(0, 'Asia/Tokyo'), toDateTime(0, 'Asia/Tokyo'))) = '1970-01-01 09:00:00' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'DateTime64 time zones';
SELECT count() FROM t_datetime64 WHERE id = 1 AND toString(if(p, toDateTime64(0, 3, 'Asia/Tokyo'), toDateTime64(0, 3, 'Asia/Tokyo'))) = '1970-01-01 09:00:00.000' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_datetime64 WHERE id = 1 AND toString(if(p, toDateTime64(0, 3, 'Asia/Tokyo'), toDateTime64(0, 3, 'Asia/Tokyo'))) = '1970-01-01 09:00:00.000' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_datetime64 WHERE id = 1 AND toString(if(p, toDateTime64(0, 3, 'Asia/Tokyo'), toDateTime64(0, 3, 'Asia/Tokyo'))) = '1970-01-01 09:00:00.000' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE t_bool;
DROP TABLE t_datetime;
DROP TABLE t_datetime64;
