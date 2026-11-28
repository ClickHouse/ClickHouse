-- With the legacy result types, `toStartOfInterval` and `dateTrunc` narrow a `Date32` argument into `Date`, which wraps
-- outside its range, and index analysis treats the rounding as monotonic only where both rounded bounds fit. The bounds
-- must be rounded to weeks that start on the day of `week_functions_starting_day` according to the function, not just to
-- Monday weeks. Each `count()` is next to its truth.

SET session_timezone = 'UTC';
SET to_start_of_interval_preserves_argument_type = 0;
SET enable_extended_results_for_datetime_functions = 0;

DROP TABLE IF EXISTS t_epoch;
CREATE TABLE t_epoch (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
-- 1970-01-01 (Thursday) ... 1970-01-10.
INSERT INTO t_epoch SELECT toDate32('1970-01-01') + number FROM numbers(10);

SELECT 'Tuesday, the first days of 1970 round to 1969-12-30, which wraps';
SET week_functions_starting_day = 'tuesday';
SELECT toTypeName(toStartOfInterval(toDate32('1970-01-05'), INTERVAL 1 WEEK)), toStartOfInterval(toDate32('1970-01-05'), INTERVAL 1 WEEK);
-- Rounded to a Monday week, 1970-01-05 fits `Date`; rounded to a Tuesday week, it doesn't.
SELECT count() FROM t_epoch WHERE toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-05');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-05')) FROM t_epoch;
SELECT count() FROM t_epoch WHERE toStartOfInterval(d, INTERVAL 1 WEEK) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) >= toDate('2100-01-01')) FROM t_epoch;

SELECT 'Tuesday, two weeks';
SELECT count() FROM t_epoch WHERE toStartOfInterval(d, INTERVAL 2 WEEK) = toDate('2149-05-29');
SELECT countIf(toStartOfInterval(d, INTERVAL 2 WEEK) = toDate('2149-05-29')) FROM t_epoch;

SELECT 'Tuesday, dateTrunc';
SELECT count() FROM t_epoch WHERE dateTrunc('week', d) = toDate('2149-06-05') SETTINGS function_date_trunc_return_type_behavior = 1;
SELECT countIf(dateTrunc('week', d) = toDate('2149-06-05')) FROM t_epoch SETTINGS function_date_trunc_return_type_behavior = 1;

SELECT 'Sunday, the end of the Date range';
SET week_functions_starting_day = 'sunday';
DROP TABLE IF EXISTS t_end;
CREATE TABLE t_end (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
-- 2149-06-01 (Sunday) ... 2149-06-10. The last day of `Date` is 2149-06-06, the Sunday 2149-06-08 starts a week past it.
INSERT INTO t_end SELECT toDate32('2149-06-01') + number FROM numbers(10);
SELECT count() FROM t_end WHERE toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-01')) FROM t_end;

SELECT 'the primary key is still used where the rounding fits';
SET week_functions_starting_day = 'tuesday';
DROP TABLE IF EXISTS t_fits;
CREATE TABLE t_fits (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, auto_statistics_types = '';
INSERT INTO t_fits SELECT toDate32('2000-01-01') + number FROM numbers(100);
SELECT max(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/100'))) BETWEEN 1 AND 99
FROM (EXPLAIN indexes = 1 SELECT d FROM t_fits WHERE toStartOfInterval(d, INTERVAL 1 WEEK) >= toDate('2000-03-01')
    SETTINGS enable_parallel_replicas = 0)
WHERE explain ILIKE '%Granules:%';

DROP TABLE t_epoch;
DROP TABLE t_end;
DROP TABLE t_fits;
