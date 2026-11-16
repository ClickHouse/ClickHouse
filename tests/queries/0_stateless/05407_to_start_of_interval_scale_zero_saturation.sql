-- A `DateTime64(0)` argument rounded to the legacy `DateTime` result saturates instead of wrapping,
-- including in the fast path where the scales of the argument and the result are equal.

SET session_timezone = 'UTC';
SET to_start_of_interval_preserves_argument_type = 0;
SET enable_extended_results_for_datetime_functions = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (d DateTime64(0)) ENGINE = Memory;
INSERT INTO t VALUES ('1900-01-01 00:00:00'), ('1969-12-31 23:59:59'), ('2000-01-01 12:34:56'), ('2107-01-01 00:00:00');

SELECT d,
    toStartOfInterval(d, INTERVAL 1 SECOND) AS s1,
    toStartOfInterval(d, INTERVAL 15 SECOND) AS s15,
    toStartOfInterval(d, INTERVAL 1 MINUTE) AS m1,
    toStartOfInterval(d, INTERVAL 1 HOUR) AS h1,
    toTypeName(h1)
FROM t ORDER BY d;

SELECT d,
    dateTrunc('second', d) AS s,
    dateTrunc('minute', d) AS m,
    dateTrunc('hour', d) AS h,
    toTypeName(h)
FROM t ORDER BY d
SETTINGS function_date_trunc_return_type_behavior = 1;

DROP TABLE t;
