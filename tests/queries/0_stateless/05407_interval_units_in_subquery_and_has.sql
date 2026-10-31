-- `IN (subquery)`, `has` and the accurate casts must compare `Interval` values of different units the way `=` does.

DROP TABLE IF EXISTS t_interval_ns;
DROP TABLE IF EXISTS t_interval_min;
DROP TABLE IF EXISTS t_interval_month;
DROP TABLE IF EXISTS t_interval_set;

CREATE TABLE t_interval_ns (time IntervalNanosecond) ENGINE = MergeTree ORDER BY time;
INSERT INTO t_interval_ns SELECT toIntervalNanosecond(number * 1000000000) FROM numbers(100000);

SELECT 'ns key';
SELECT count() FROM t_interval_ns WHERE time = INTERVAL 510 MINUTE;
SELECT count() FROM t_interval_ns WHERE time IN (SELECT INTERVAL 510 MINUTE) SETTINGS use_index_for_in_with_subqueries = 1, force_primary_key = 1;
SELECT count() FROM t_interval_ns WHERE time IN (SELECT INTERVAL 510 MINUTE) SETTINGS use_index_for_in_with_subqueries = 0;
SELECT count() FROM t_interval_ns WHERE time NOT IN (SELECT INTERVAL 510 MINUTE);
SELECT count() FROM t_interval_ns WHERE has([INTERVAL 510 MINUTE], time);
SELECT count() FROM t_interval_ns WHERE notHas([INTERVAL 510 MINUTE], time);

CREATE TABLE t_interval_min (m IntervalMinute) ENGINE = MergeTree ORDER BY m;
INSERT INTO t_interval_min SELECT toIntervalMinute(number) FROM numbers(1000);

SELECT 'minute key, nanosecond set';
SELECT count() FROM t_interval_min WHERE m IN (SELECT toIntervalNanosecond(30600000000000)) SETTINGS use_index_for_in_with_subqueries = 1, force_primary_key = 1;
SELECT count() FROM t_interval_min WHERE m IN (SELECT toIntervalNanosecond(30600000000001)) SETTINGS use_index_for_in_with_subqueries = 1, force_primary_key = 1;
SELECT count() FROM t_interval_min WHERE m NOT IN (SELECT toIntervalNanosecond(30600000000001));

CREATE TABLE t_interval_month (m IntervalMonth) ENGINE = MergeTree ORDER BY m;
INSERT INTO t_interval_month SELECT toIntervalMonth(number) FROM numbers(100);

SELECT 'month key, day set';
SELECT count() FROM t_interval_month WHERE m IN (SELECT INTERVAL 28 DAY) SETTINGS use_index_for_in_with_subqueries = 1, force_primary_key = 1;
SELECT count() FROM t_interval_month WHERE m NOT IN (SELECT INTERVAL 28 DAY);
SELECT count() FROM t_interval_month WHERE m IN (SELECT INTERVAL 2 YEAR) SETTINGS use_index_for_in_with_subqueries = 1, force_primary_key = 1;

SELECT 'IN';
SELECT toIntervalSecond(90) IN (SELECT INTERVAL 1 MINUTE);
SELECT toIntervalSecond(120) IN (SELECT INTERVAL 2 MINUTE);
SELECT toIntervalSecond(-90) IN (SELECT INTERVAL -1 MINUTE);
SELECT INTERVAL 1 MINUTE IN (SELECT toIntervalSecond(60));
SELECT toIntervalSecond(90) NOT IN (SELECT INTERVAL 1 MINUTE);
SELECT toIntervalSecond(90) GLOBAL IN (SELECT INTERVAL 1 MINUTE);
SELECT nullIn(toIntervalSecond(90), (SELECT INTERVAL 1 MINUTE));
SELECT toIntervalSecond(90) IN (SELECT INTERVAL 1 MINUTE) SETTINGS transform_null_in = 1;
SELECT toNullable(toIntervalSecond(90)) IN (SELECT INTERVAL 1 MINUTE);
SELECT toLowCardinality(toIntervalSecond(90)) IN (SELECT INTERVAL 1 MINUTE);
SELECT (toIntervalSecond(90), 1) IN (SELECT INTERVAL 1 MINUTE, 1);
SELECT (toIntervalSecond(120), 1) IN (SELECT (INTERVAL 2 MINUTE, 1));
SELECT (toIntervalSecond(90), 1) IN (SELECT (INTERVAL 1 MINUTE, 1)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT [toIntervalSecond(120)] IN (SELECT [INTERVAL 2 MINUTE]);
SELECT [toIntervalSecond(90)] IN (SELECT [INTERVAL 1 MINUTE]); -- { serverError CANNOT_CONVERT_TYPE }
SELECT map(1, toIntervalSecond(120)) IN (SELECT map(1, INTERVAL 2 MINUTE));
SELECT map(1, toIntervalSecond(90)) IN (SELECT map(1, INTERVAL 1 MINUTE)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT INTERVAL 13 MONTH IN (SELECT INTERVAL 1 YEAR);
SELECT INTERVAL 28 DAY IN (SELECT INTERVAL 1 MONTH);
SELECT toIntervalWeek(20000) IN (SELECT toIntervalNanosecond(-6350744073709551616));

CREATE TABLE t_interval_set (x IntervalMinute) ENGINE = Set;
INSERT INTO t_interval_set VALUES (1);
SELECT toIntervalSecond(90) IN t_interval_set, toIntervalSecond(60) IN t_interval_set;

SELECT 'accurate casts';
SELECT accurateCastOrNull(toIntervalSecond(90), 'IntervalMinute'), accurateCastOrNull(toIntervalSecond(120), 'IntervalMinute');
SELECT accurateCastOrNull(toIntervalWeek(20000), 'IntervalNanosecond'), accurateCastOrNull(INTERVAL 28 DAY, 'IntervalMonth');
SELECT accurateCastOrDefault(toIntervalSecond(90), 'IntervalMinute');
SELECT accurateCast(toIntervalSecond(120), 'IntervalMinute'), accurateCast(INTERVAL 1 YEAR, 'IntervalMonth');
SELECT accurateCast(toIntervalSecond(90), 'IntervalMinute'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(INTERVAL 1 MONTH, 'IntervalDay'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(toIntervalSecond(90), 'IntervalMinute');

SELECT 'has';
SELECT has([INTERVAL 1 MINUTE], toIntervalSecond(60)), has([INTERVAL 1 MINUTE], toIntervalSecond(61)), has([toIntervalSecond(60)], INTERVAL 1 MINUTE);
SELECT has(materialize([INTERVAL 1 MINUTE]), materialize(toIntervalSecond(60))), has([INTERVAL 1 MINUTE, NULL], toIntervalSecond(60)), has([INTERVAL 1 MINUTE, NULL], toIntervalSecond(90));
SELECT has([INTERVAL 1 YEAR], INTERVAL 12 MONTH), notHas([INTERVAL 1 MINUTE], toIntervalSecond(60));
SELECT indexOf([INTERVAL 1 MINUTE, INTERVAL 2 MINUTE], toIntervalSecond(120)), indexOfAssumeSorted([INTERVAL 1 MINUTE, INTERVAL 2 MINUTE], toIntervalSecond(120));
SELECT countEqual([INTERVAL 1 MINUTE, INTERVAL 1 MINUTE], toIntervalSecond(60));
SELECT mapContainsKey(map(INTERVAL 1 MINUTE, 1), toIntervalSecond(60)), mapContainsValue(map(1, INTERVAL 1 MINUTE), toIntervalSecond(60));
SELECT has(materialize([toLowCardinality(toIntervalMinute(1))]), materialize(toIntervalSecond(60))), has([toLowCardinality(toIntervalMinute(1))], toIntervalSecond(90));
SELECT indexOfAssumeSorted([toIntervalWeek(1), toIntervalWeek(2), toIntervalWeek(3)], toIntervalNanosecond(1209600000000000));
SELECT indexOf(arr, n), indexOfAssumeSorted(arr, n), has([toIntervalNanosecond(-6350744073709551616)], toIntervalWeek(20000))
FROM (SELECT [toIntervalWeek(10000), toIntervalWeek(16000), toIntervalWeek(20000)] AS arr, toIntervalNanosecond(-8769944073709551616) AS n);
SELECT has([INTERVAL 1 MONTH], INTERVAL 30 DAY); -- { serverError NO_COMMON_TYPE }

SELECT 'has composite';
SELECT has([(INTERVAL 1 MINUTE, 7)], (toIntervalSecond(60), 7)), has([(INTERVAL 1 MINUTE, 7)], (toIntervalSecond(1), 7));
SELECT has([[INTERVAL 1 MINUTE]], [toIntervalSecond(60)]), has([[INTERVAL 1 MINUTE]], [toIntervalSecond(1)]);
SELECT has([INTERVAL '1:30' HOUR TO MINUTE], (toIntervalMinute(60), toIntervalSecond(1800))), has([INTERVAL '1:30' HOUR TO MINUTE], (toIntervalMinute(1), toIntervalSecond(30)));
SELECT indexOf([(INTERVAL 1 MINUTE, 1), (INTERVAL 2 MINUTE, 1)], (toIntervalSecond(120), 1)), indexOfAssumeSorted([(INTERVAL 1 MINUTE, 1), (INTERVAL 2 MINUTE, 1)], (toIntervalSecond(120), 1)), countEqual([(INTERVAL 1 MINUTE, 1), (INTERVAL 1 MINUTE, 1)], (toIntervalSecond(60), 1));
SELECT has([(INTERVAL 1 MINUTE, 7)], (toIntervalSecond(number * 30), 7)) FROM numbers(3) ORDER BY number;

DROP TABLE t_interval_ns;
DROP TABLE t_interval_min;
DROP TABLE t_interval_month;
DROP TABLE t_interval_set;
