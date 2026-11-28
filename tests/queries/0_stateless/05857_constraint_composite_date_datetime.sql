-- `d <= u AND u <= l` does not give `d <= l` for a `Date` in a tuple or an array: the date is converted to a time in the time zone of the other side

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

DROP TABLE IF EXISTS t_tuple_date;
DROP TABLE IF EXISTS t_array_date;

CREATE TABLE t_tuple_date
(
    d Tuple(Date),
    u Tuple(DateTime('UTC')),
    l Tuple(DateTime('America/Los_Angeles')),
    CONSTRAINT c CHECK d <= u AND u <= l
) ENGINE = Memory;
INSERT INTO t_tuple_date VALUES (tuple('2020-01-01'), tuple('2020-01-01 01:00:00'), tuple('2019-12-31 18:00:00'));

SELECT 'tuple', count() FROM t_tuple_date WHERE d <= l SETTINGS optimize_using_constraints = 0;
SELECT 'tuple', count() FROM t_tuple_date WHERE d <= l SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_array_date
(
    d Array(Date),
    u Array(DateTime('UTC')),
    l Array(DateTime('America/Los_Angeles')),
    CONSTRAINT c CHECK d <= u AND u <= l
) ENGINE = Memory;
INSERT INTO t_array_date VALUES (['2020-01-01'], ['2020-01-01 01:00:00'], ['2019-12-31 18:00:00']);

SELECT 'array', count() FROM t_array_date WHERE d <= l SETTINGS optimize_using_constraints = 0;
SELECT 'array', count() FROM t_array_date WHERE d <= l SETTINGS optimize_using_constraints = 1;

DROP TABLE t_tuple_date;
DROP TABLE t_array_date;
