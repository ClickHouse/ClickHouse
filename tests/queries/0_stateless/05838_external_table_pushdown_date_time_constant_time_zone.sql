-- Tags: no-fasttest
-- A date-time constant in another time zone than the compared column of a MySQL table is pushed down
-- as the same instant, so the rows match the MergeTree source.

SET send_logs_level = 'fatal'; -- failed connection tries are ok, if it succeeded after retry.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ext;
DROP TABLE IF EXISTS ext_nz;
DROP TABLE IF EXISTS src;

CREATE TABLE src (id UInt8, d Date, dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'), ndt Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1, '2026-01-01', '2026-01-01 00:00:00', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), (2, '2026-01-01', '2026-01-01 09:00:00', '2026-01-01 09:00:00', '2026-01-01 09:00:00'), (3, '2026-01-02', '2026-01-02 00:00:00', '2026-01-01 00:00:00.500', NULL), (4, '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00');
CREATE TABLE ext (id UInt8, d Date, dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'), ndt Nullable(DateTime('UTC')))
    ENGINE = MySQL('127.0.0.1:9004', currentDatabase(), 'src', 'default', '');
CREATE TABLE ext_nz (id UInt8, dt DateTime, dt64 DateTime64(3)) ENGINE = MySQL('127.0.0.1:9004', currentDatabase(), 'src', 'default', '');

SELECT arraySort(groupArray(id)) FROM ext WHERE dt = toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo') SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo') = dt SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt = toDateTime('2026-01-01 09:00:00') SETTINGS external_table_strict_query = 1, session_timezone = 'Asia/Tokyo';
SELECT arraySort(groupArray(id)) FROM ext WHERE dt IN (toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo'), toDateTime('2026-01-01 10:00:00', 'Asia/Tokyo')) SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt NOT IN (toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo')) SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt >= toDateTime('2026-01-01 08:30:00', 'Asia/Tokyo') AND dt < toDateTime('2026-01-01 09:30:00', 'Asia/Tokyo') SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt = toDateTime64('2026-01-01 09:00:00', 3, 'Asia/Tokyo') SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt64 = toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo') SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE ndt = toNullable(toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo')) SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE (id, dt) IN ((1, toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo'))) SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt = toTimeZone(toDateTime('2026-01-01 00:00:00', 'UTC'), 'Asia/Tokyo') SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt = toDateTime('2026-01-01 00:00:00', 'UTC') SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext_nz WHERE dt = toDateTime('2026-01-01 09:00:00') SETTINGS external_table_strict_query = 1, session_timezone = 'Asia/Tokyo';
SELECT arraySort(groupArray(id)) FROM ext_nz WHERE dt IN (toDateTime('2026-01-01 09:00:00'), toDateTime('2026-01-01 10:00:00')) SETTINGS external_table_strict_query = 1, session_timezone = 'Asia/Tokyo';
SELECT arraySort(groupArray(id)) FROM ext WHERE dt64 = toDateTime64('2026-01-01 09:00:00.500', 3, 'Asia/Tokyo') SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE dt64 IN (toDateTime64('2026-01-01 09:00:00.500', 3, 'Asia/Tokyo')) SETTINGS external_table_strict_query = 1;
SELECT arraySort(groupArray(id)) FROM ext WHERE (d, dt) IN ((toDate('2026-01-01'), toDateTime('2026-01-01 09:00:00', 'Asia/Tokyo')), (toDate('2026-01-02'), toDateTime('2026-01-02 09:00:00', 'Asia/Tokyo'))) SETTINGS external_table_strict_query = 1;
-- MySQL reads a DateTime64 without an explicit zone in the session zone: the pushed filter keeps the row the local one keeps.
SELECT arraySort(groupArray(id)) FROM (SELECT * FROM ext_nz LIMIT 1000) WHERE dt64 = toDateTime64('2026-01-01 09:00:00', 3) SETTINGS session_timezone = 'Asia/Tokyo';
SELECT arraySort(groupArray(id)) FROM ext_nz WHERE dt64 = toDateTime64('2026-01-01 09:00:00', 3) SETTINGS external_table_strict_query = 1, session_timezone = 'Asia/Tokyo';
-- No member fits `DateTime`, so the set is empty and the condition is applied locally.
SELECT arraySort(groupArray(id)) FROM ext WHERE dt NOT IN (toDateTime64('1970-01-01 00:00:00', 0, 'Asia/Tokyo'));
SELECT arraySort(groupArray(id)) FROM ext WHERE ndt NOT IN (toDateTime64('1970-01-01 00:00:00', 0, 'Asia/Tokyo'));
SELECT arraySort(groupArray(id)) FROM ext WHERE dt NOT IN (toDateTime64('1970-01-01 00:00:00', 0, 'Asia/Tokyo')) SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }

DROP TABLE ext;
DROP TABLE ext_nz;
DROP TABLE src;
