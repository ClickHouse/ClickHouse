-- A `Date` or `Date32` converted to `Time` / `Time64` is midnight, as `CAST` gives, also when nested,
-- in the set of `IN` and in the `values` table function. The day's Unix timestamp must not leak in
-- (for `2106-02-07` it wrapped to `-06:28:16` in the `Int32` storage of `Time`).

SET enable_time_time64_type = 1;
SET session_timezone = 'UTC';

SELECT CAST([toDate32('2106-02-07')], 'Array(Time)'), CAST(toDate('2020-01-02'), 'Time64(3)');

SELECT * FROM values('x Array(Time)', [toDate32('2106-02-07')], [toDate('2020-01-02')]);
SELECT * FROM values('x Array(Time64(3))', [toDate32('2106-02-07')], [toDate('2020-01-02')]);
SELECT * FROM values('x Time', toDate('2020-01-02'));

SELECT [toTime('-06:28:16')] IN ([toDate32('2106-02-07')]);
SELECT [toTime('00:00:00')] IN ([toDate32('2106-02-07')]);
SELECT [toTime('00:00:00')] IN ([toDate('2020-01-02')]);
SELECT toTime('00:00:00') IN (toDate('2020-01-02'));
SELECT toTime64('00:00:00', 3) IN (toDate32('2106-02-07'));

DROP TABLE IF EXISTS t_time_arr;
CREATE TABLE t_time_arr (x Array(Time)) ENGINE = Memory;
INSERT INTO t_time_arr VALUES ([toTime('-06:28:16')]), ([toTime('00:00:00')]);
SELECT x FROM t_time_arr WHERE x IN ([toDate32('2106-02-07')]) ORDER BY x;
SELECT x FROM t_time_arr WHERE x NOT IN ([toDate32('2106-02-07')]) ORDER BY x;
DROP TABLE t_time_arr;
