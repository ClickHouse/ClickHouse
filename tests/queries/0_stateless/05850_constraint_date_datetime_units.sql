-- a `Date` counts days and a `DateTime` counts seconds: a comparison of the two converts the date, the constraint bounds must not be compared as they are

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0;

DROP TABLE IF EXISTS t_date_bound;
DROP TABLE IF EXISTS t_datetime_bound;
DROP TABLE IF EXISTS t_date32_bound;

-- the constraint bound is a date, the query constant is a time
CREATE TABLE t_date_bound (d Date, CONSTRAINT c CHECK d = toDate('2020-01-02')) ENGINE = Memory;
INSERT INTO t_date_bound VALUES ('2020-01-02');
SELECT 'date bound', count() FROM t_date_bound WHERE d < toDateTime('2020-01-01 12:00:00', 'UTC') SETTINGS optimize_using_constraints = 0;
SELECT 'date bound', count() FROM t_date_bound WHERE d < toDateTime('2020-01-01 12:00:00', 'UTC') SETTINGS optimize_using_constraints = 1;
SELECT 'date bound', count() FROM t_date_bound WHERE d < toDateTime64('2020-01-01 12:00:00', 3, 'UTC') SETTINGS optimize_using_constraints = 0;
SELECT 'date bound', count() FROM t_date_bound WHERE d < toDateTime64('2020-01-01 12:00:00', 3, 'UTC') SETTINGS optimize_using_constraints = 1;

-- the time bound reaches the date through `d = dt`, the query compares two dates
CREATE TABLE t_datetime_bound (d Date, dt DateTime('UTC'), CONSTRAINT c1 CHECK d = dt, CONSTRAINT c2 CHECK dt = toDateTime('2020-01-02 00:00:00', 'UTC')) ENGINE = Memory;
INSERT INTO t_datetime_bound VALUES ('2020-01-02', '2020-01-02 00:00:00');
SELECT 'datetime bound', count() FROM t_datetime_bound WHERE d < toDate('2020-01-03') SETTINGS optimize_using_constraints = 0;
SELECT 'datetime bound', count() FROM t_datetime_bound WHERE d < toDate('2020-01-03') SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_date32_bound (d Date32, dt DateTime64(3, 'UTC'), CONSTRAINT c1 CHECK d = dt, CONSTRAINT c2 CHECK dt = toDateTime64('2020-01-02 00:00:00', 3, 'UTC')) ENGINE = Memory;
INSERT INTO t_date32_bound VALUES ('2020-01-02', '2020-01-02 00:00:00');
SELECT 'date32 bound', count() FROM t_date32_bound WHERE d < toDate32('2020-01-03') SETTINGS optimize_using_constraints = 0;
SELECT 'date32 bound', count() FROM t_date32_bound WHERE d < toDate32('2020-01-03') SETTINGS optimize_using_constraints = 1;
SELECT 'date32 bound', count() FROM t_date32_bound WHERE d < toDateTime('2020-01-01 12:00:00', 'UTC') SETTINGS optimize_using_constraints = 1;

DROP TABLE t_date_bound;
DROP TABLE t_datetime_bound;
DROP TABLE t_date32_bound;
