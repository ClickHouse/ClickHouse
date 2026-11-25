-- Tags: no-parallel
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/119578:
-- `toRelativeHourNum` must be non-decreasing across the Unix epoch in whole-hour time zones
-- so MergeTree partition pruning and primary-key analysis do not drop rows.

SET enable_extended_results_for_datetime_functions = 0;

SELECT '-- 1. Monotonicity across epoch (UTC, America/New_York, Europe/London, Asia/Kolkata)';
SELECT 'America/New_York', arraySort(groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'America/New_York'))) = groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'America/New_York'))
FROM (SELECT arrayJoin(['1969-12-31 20:00:00.500', '1969-12-31 21:00:00.000', '1969-12-31 22:30:00.500', '1969-12-31 23:59:59.999', '1970-01-01 00:00:00.000', '1970-01-01 01:30:00.000', '1970-01-01 02:00:00.000', '1970-01-01 05:00:00.000']) AS s ORDER BY s);
SELECT 'Asia/Kolkata', arraySort(groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'Asia/Kolkata'))) = groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'Asia/Kolkata'))
FROM (SELECT arrayJoin(['1969-12-31 20:00:00.500', '1969-12-31 21:00:00.000', '1969-12-31 22:30:00.500', '1969-12-31 23:59:59.999', '1970-01-01 00:00:00.000', '1970-01-01 01:30:00.000', '1970-01-01 02:00:00.000', '1970-01-01 05:00:00.000']) AS s ORDER BY s);
SELECT 'Europe/London', arraySort(groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'Europe/London'))) = groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'Europe/London'))
FROM (SELECT arrayJoin(['1969-12-31 20:00:00.500', '1969-12-31 21:00:00.000', '1969-12-31 22:30:00.500', '1969-12-31 23:59:59.999', '1970-01-01 00:00:00.000', '1970-01-01 01:30:00.000', '1970-01-01 02:00:00.000', '1970-01-01 05:00:00.000']) AS s ORDER BY s);
SELECT 'UTC', arraySort(groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'UTC'))) = groupArray(toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'UTC'))
FROM (SELECT arrayJoin(['1969-12-31 20:00:00.500', '1969-12-31 21:00:00.000', '1969-12-31 22:30:00.500', '1969-12-31 23:59:59.999', '1970-01-01 00:00:00.000', '1970-01-01 01:30:00.000', '1970-01-01 02:00:00.000', '1970-01-01 05:00:00.000']) AS s ORDER BY s);

SELECT '-- 2. Pre-epoch UTC values clamp to 0 (matching toRelativeMinuteNum)';
SELECT
    s,
    toRelativeHourNum(toDateTime64(s, 3, 'UTC'), 'UTC')
FROM
(
    SELECT arrayJoin([
        '1969-12-31 20:00:00.000',
        '1969-12-31 23:00:00.000',
        '1969-12-31 23:59:59.999',
        '1970-01-01 00:00:00.000',
        '1970-01-01 01:00:00.000',
        '1970-01-01 02:00:00.000'
    ]) AS s
)
ORDER BY s;

SELECT '-- 3. MergeTree PARTITION BY toRelativeHourNum(d) range pruning (UTC)';
DROP TABLE IF EXISTS t_part_119578;
CREATE TABLE t_part_119578 (d DateTime64(0, 'UTC')) ENGINE = MergeTree PARTITION BY toRelativeHourNum(d) ORDER BY d;
INSERT INTO t_part_119578 VALUES ('1969-12-31 21:00:00'),('1969-12-31 22:00:00'),('1969-12-31 23:00:00'),('1970-01-01 02:00:00'),('1970-01-05 00:00:00');
SELECT
    (SELECT count() FROM t_part_119578 WHERE d >= toDateTime64('1969-12-31 21:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC')) AS pruned,
    (SELECT countIf(d >= toDateTime64('1969-12-31 21:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC')) FROM t_part_119578) AS full_scan;
DROP TABLE t_part_119578;

SELECT '-- 4. MergeTree ORDER BY d primary-key pruning (UTC)';
DROP TABLE IF EXISTS t_pk_119578;
CREATE TABLE t_pk_119578 (d DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 2;
INSERT INTO t_pk_119578 VALUES ('1969-12-31 20:00:00'),('1969-12-31 21:00:00'),('1969-12-31 23:00:00'),('1970-01-01 01:30:00'),('1970-01-01 05:00:00'),('1970-01-01 07:00:00');
SELECT
    (SELECT count() FROM t_pk_119578 WHERE toRelativeHourNum(d) = 0 SETTINGS force_primary_key = 1) AS pruned_0,
    (SELECT countIf(toRelativeHourNum(d) = 0) FROM t_pk_119578) AS full_0,
    (SELECT count() FROM t_pk_119578 WHERE toRelativeHourNum(d) = 23 SETTINGS force_primary_key = 1) AS pruned_23,
    (SELECT countIf(toRelativeHourNum(d) = 23) FROM t_pk_119578) AS full_23;
DROP TABLE t_pk_119578;

SELECT '-- 5. Date32 ORDER BY d primary-key pruning (America/New_York)';
DROP TABLE IF EXISTS t_d32_119578;
CREATE TABLE t_d32_119578 (d Date32) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_d32_119578 VALUES ('1969-12-30'),('1969-12-31'),('1970-01-01'),('1970-01-02');
SELECT
    (SELECT count() FROM t_d32_119578 WHERE toRelativeHourNum(d, 'America/New_York') <= 5 SETTINGS force_primary_key = 1) AS pruned,
    (SELECT countIf(toRelativeHourNum(d, 'America/New_York') <= 5) FROM t_d32_119578) AS full_scan;
DROP TABLE t_d32_119578;

SELECT '-- 6. Extended results & dateDiff remain consistent';
SELECT
    toRelativeHourNum(toDateTime64('1969-12-31 23:00:00', 0, 'UTC')) AS ext_pre,
    toRelativeHourNum(toDateTime64('1970-01-01 00:00:00', 0, 'UTC')) AS ext_epoch,
    dateDiff('hour', toDateTime64('1969-12-31 23:00:00', 0, 'UTC'), toDateTime64('1970-01-01 01:00:00', 0, 'UTC')) AS diff_hr
SETTINGS enable_extended_results_for_datetime_functions = 1;
