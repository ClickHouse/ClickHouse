-- Primary key analysis of `IN` over a single numeric key column uses a typed binary search over the set.
-- Each query compares the result with the primary key against the result without it.

SET max_threads = 1;

DROP TABLE IF EXISTS t_u64;
DROP TABLE IF EXISTS t_i32;
DROP TABLE IF EXISTS t_f64;
DROP TABLE IF EXISTS t_date;
DROP TABLE IF EXISTS t_dt;
DROP TABLE IF EXISTS t_tuple;

SELECT '--- UInt64';
CREATE TABLE t_u64 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_u64 SELECT number * 3 FROM numbers(1000);
INSERT INTO t_u64 VALUES (18446744073709551615), (18446744073709551614), (0);

SELECT count(), (SELECT count() FROM t_u64 WHERE k IN (0, 3, 4, 2997, 18446744073709551615) SETTINGS use_primary_key = 0)
    FROM t_u64 WHERE k IN (0, 3, 4, 2997, 18446744073709551615);
SELECT count() FROM t_u64 WHERE k IN (0, 3, 4, 2997, 18446744073709551615) SETTINGS max_rows_to_read = 24;
SELECT count(), (SELECT count() FROM t_u64 WHERE k IN (SELECT number * 7 FROM numbers(500)) SETTINGS use_primary_key = 0)
    FROM t_u64 WHERE k IN (SELECT number * 7 FROM numbers(500));
-- None of the values is in the table, and only the marks around them are read.
SELECT count() FROM t_u64 WHERE k IN (1, 2, 5000, 18446744073709551613) SETTINGS max_rows_to_read = 16;
SELECT count() FROM t_u64 WHERE k NOT IN (0, 3, 6);

SELECT '--- Int32';
CREATE TABLE t_i32 (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_i32 SELECT number - 500 FROM numbers(1000);
INSERT INTO t_i32 VALUES (-2147483648), (2147483647);

SELECT count(), (SELECT count() FROM t_i32 WHERE k IN (-2147483648, -500, -1, 0, 1, 499, 2147483647) SETTINGS use_primary_key = 0)
    FROM t_i32 WHERE k IN (-2147483648, -500, -1, 0, 1, 499, 2147483647);
SELECT count() FROM t_i32 WHERE k IN (-2147483648, -500, -1, 0, 1, 499, 2147483647) SETTINGS max_rows_to_read = 40;
-- Through a monotonic function: the set is in the function's type.
SELECT count(), (SELECT count() FROM t_i32 WHERE intDiv(k, 10) IN (-50, -1, 0, 49) SETTINGS use_primary_key = 0)
    FROM t_i32 WHERE intDiv(k, 10) IN (-50, -1, 0, 49);
SELECT count(), (SELECT count() FROM t_i32 WHERE -k IN (-499, 0, 500) SETTINGS use_primary_key = 0)
    FROM t_i32 WHERE -k IN (-499, 0, 500);

SELECT '--- Float64';
CREATE TABLE t_f64 (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4, allow_floating_point_partition_key = 1;
INSERT INTO t_f64 SELECT (number - 500) / 4 FROM numbers(1000);
INSERT INTO t_f64 VALUES (nan), (nan), (inf), (-inf), (-0.), (0.);

SELECT count(), (SELECT count() FROM t_f64 WHERE k IN (-inf, -125, -0.25, 0, 0.1, 124.75, inf, nan) SETTINGS use_primary_key = 0)
    FROM t_f64 WHERE k IN (-inf, -125, -0.25, 0, 0.1, 124.75, inf, nan);
SELECT count(), (SELECT count() FROM t_f64 WHERE k NOT IN (0, inf) SETTINGS use_primary_key = 0)
    FROM t_f64 WHERE k NOT IN (0, inf);

SELECT '--- Date';
CREATE TABLE t_date (k Date) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_date SELECT toDate('2020-01-01') + number FROM numbers(1000);

SELECT count(), (SELECT count() FROM t_date WHERE k IN ('2020-01-01', '2020-06-15', '2022-09-26', '2030-01-01') SETTINGS use_primary_key = 0)
    FROM t_date WHERE k IN ('2020-01-01', '2020-06-15', '2022-09-26', '2030-01-01');
SELECT count() FROM t_date WHERE k IN ('2020-01-01', '2020-06-15', '2022-09-26', '2030-01-01') SETTINGS max_rows_to_read = 16;
SELECT count(), (SELECT count() FROM t_date WHERE toYYYYMM(k) IN (202001, 202112) SETTINGS use_primary_key = 0)
    FROM t_date WHERE toYYYYMM(k) IN (202001, 202112);

SELECT '--- DateTime with a function that rounds';
CREATE TABLE t_dt (k DateTime('UTC')) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_dt SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(1000);

SELECT count(), (SELECT count() FROM t_dt WHERE toDate(k) IN ('2020-01-02', '2020-02-11') SETTINGS use_primary_key = 0)
    FROM t_dt WHERE toDate(k) IN ('2020-01-02', '2020-02-11');
SELECT count(), (SELECT count() FROM t_dt WHERE toStartOfHour(k) IN (toDateTime('2020-01-01 05:00:00', 'UTC')) SETTINGS use_primary_key = 0)
    FROM t_dt WHERE toStartOfHour(k) IN (toDateTime('2020-01-01 05:00:00', 'UTC'));

SELECT '--- Compound key and tuple set keep the generic search';
CREATE TABLE t_tuple (a UInt32, b Int64) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 4;
INSERT INTO t_tuple SELECT intDiv(number, 10), number % 10 - 5 FROM numbers(1000);

SELECT count(), (SELECT count() FROM t_tuple WHERE (a, b) IN ((0, -5), (3, 0), (99, 4)) SETTINGS use_primary_key = 0)
    FROM t_tuple WHERE (a, b) IN ((0, -5), (3, 0), (99, 4));
SELECT count(), (SELECT count() FROM t_tuple WHERE a IN (0, 50, 99) SETTINGS use_primary_key = 0)
    FROM t_tuple WHERE a IN (0, 50, 99);

DROP TABLE t_u64;
DROP TABLE t_i32;
DROP TABLE t_f64;
DROP TABLE t_date;
DROP TABLE t_dt;
DROP TABLE t_tuple;
