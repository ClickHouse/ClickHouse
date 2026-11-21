-- a string constant compared with a column of another type is read as a value of that type, not ordered as a string

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0;

DROP TABLE IF EXISTS t_number;
DROP TABLE IF EXISTS t_enum_number;
DROP TABLE IF EXISTS t_date;
DROP TABLE IF EXISTS t_datetime_bound;
DROP TABLE IF EXISTS t_datetime;
DROP TABLE IF EXISTS t_mixed;

-- '9' and '10' are the numbers 9 and 10 for `n`
CREATE TABLE t_number (n UInt8, CONSTRAINT c CHECK n = '9') ENGINE = Memory;
INSERT INTO t_number VALUES (9);
SELECT 'number', count() FROM t_number WHERE n < '10' SETTINGS optimize_using_constraints = 0;
SELECT 'number', count() FROM t_number WHERE n < '10' SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_enum_number (e Enum8('9' = 1, 'a' = 9), n UInt8, CONSTRAINT c CHECK e = n AND n = '9') ENGINE = Memory;
INSERT INTO t_enum_number VALUES ('a', 9);
SELECT 'enum number', count() FROM t_enum_number WHERE n < '10' SETTINGS optimize_using_constraints = 0;
SELECT 'enum number', count() FROM t_enum_number WHERE n < '10' SETTINGS optimize_using_constraints = 1;
SELECT 'enum number', e FROM t_enum_number WHERE e = 9 SETTINGS optimize_using_constraints = 1;

-- '2020-1-5' is the date 2020-01-05
CREATE TABLE t_date (d Date, CONSTRAINT c CHECK d = '2020-1-5') ENGINE = Memory;
INSERT INTO t_date VALUES ('2020-01-05');
SELECT 'date', count() FROM t_date WHERE d < '2020-01-10' SETTINGS optimize_using_constraints = 0;
SELECT 'date', count() FROM t_date WHERE d < '2020-01-10' SETTINGS optimize_using_constraints = 1;

-- the constraint bound is a time, the query constant is a number
CREATE TABLE t_datetime_bound (dt DateTime('UTC'), CONSTRAINT c CHECK dt = '2020-01-01 00:00:00') ENGINE = Memory;
INSERT INTO t_datetime_bound VALUES ('2020-01-01 00:00:00');
SELECT 'datetime bound', count() FROM t_datetime_bound WHERE dt > toDateTime('2019-01-01 00:00:00', 'UTC') SETTINGS optimize_using_constraints = 0;
SELECT 'datetime bound', count() FROM t_datetime_bound WHERE dt > toDateTime('2019-01-01 00:00:00', 'UTC') SETTINGS optimize_using_constraints = 1;

-- the query constant is a time, the constraint bound is a number
CREATE TABLE t_datetime (dt DateTime('UTC'), CONSTRAINT c CHECK dt >= 1600000000) ENGINE = Memory;
INSERT INTO t_datetime VALUES (1600000000);
SELECT 'datetime', count() FROM t_datetime WHERE dt < '2021-01-01 00:00:00' SETTINGS optimize_using_constraints = 0;
SELECT 'datetime', count() FROM t_datetime WHERE dt < '2021-01-01 00:00:00' SETTINGS optimize_using_constraints = 1;
-- the index hint must not compare the bound 1600000000 with the string
SELECT 'datetime', count() FROM t_datetime WHERE dt < '2021-01-01 00:00:00' SETTINGS optimize_using_constraints = 1, optimize_append_index = 1;

-- the graph also orders the bounds of different columns against each other
CREATE TABLE t_mixed (d Date, x UInt8, s LowCardinality(String), CONSTRAINT c CHECK d = '2020-01-01' AND x = 5 AND s = 'abc') ENGINE = Memory;
INSERT INTO t_mixed VALUES ('2020-01-01', 5, 'abc');
SELECT 'mixed', count() FROM t_mixed WHERE d < '2021-01-01' SETTINGS optimize_using_constraints = 0;
SELECT 'mixed', count() FROM t_mixed WHERE d < '2021-01-01' SETTINGS optimize_using_constraints = 1;
SELECT 'mixed', count() FROM t_mixed WHERE s < 'b' AND x < 6 SETTINGS optimize_using_constraints = 1;

DROP TABLE t_number;
DROP TABLE t_enum_number;
DROP TABLE t_date;
DROP TABLE t_datetime_bound;
DROP TABLE t_datetime;
DROP TABLE t_mixed;
