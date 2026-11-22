-- a column equal to a column with another time zone must keep its own time zone after substitution

SET convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_append_index = 0;

DROP TABLE IF EXISTS t_sub_datetime;
DROP TABLE IF EXISTS t_sub_datetime64;
DROP TABLE IF EXISTS t_sub_tuple;

CREATE TABLE t_sub_datetime (dt DateTime('UTC'), k DateTime('Asia/Tokyo'), CONSTRAINT c CHECK dt = k) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sub_datetime VALUES ('2020-01-01 20:00:00', '2020-01-02 05:00:00');
SELECT 'datetime filter', count() FROM t_sub_datetime WHERE toDate(dt) = toDate('2020-01-01') SETTINGS optimize_substitute_columns = 0;
SELECT 'datetime filter', count() FROM t_sub_datetime WHERE toDate(dt) = toDate('2020-01-01') SETTINGS optimize_substitute_columns = 1;
SELECT 'datetime projection', toString(dt) FROM t_sub_datetime WHERE k > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'datetime projection', toString(dt) FROM t_sub_datetime WHERE k > 0 SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_sub_datetime64 (dt DateTime64(3, 'UTC'), k DateTime64(3, 'Asia/Tokyo'), CONSTRAINT c CHECK dt = k) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sub_datetime64 VALUES ('2020-01-01 20:00:00', '2020-01-02 05:00:00');
SELECT 'datetime64 filter', count() FROM t_sub_datetime64 WHERE toDate(dt) = toDate('2020-01-01') SETTINGS optimize_substitute_columns = 0;
SELECT 'datetime64 filter', count() FROM t_sub_datetime64 WHERE toDate(dt) = toDate('2020-01-01') SETTINGS optimize_substitute_columns = 1;
SELECT 'datetime64 projection', toString(dt) FROM t_sub_datetime64 WHERE k > 0 SETTINGS optimize_substitute_columns = 0;
SELECT 'datetime64 projection', toString(dt) FROM t_sub_datetime64 WHERE k > 0 SETTINGS optimize_substitute_columns = 1;

CREATE TABLE t_sub_tuple (dt Tuple(DateTime('UTC')), k Tuple(DateTime('Asia/Tokyo')), CONSTRAINT c CHECK dt = k) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sub_tuple VALUES (tuple('2020-01-01 20:00:00'), tuple('2020-01-02 05:00:00'));
SELECT 'tuple filter', count() FROM t_sub_tuple WHERE toDate(dt.1) = toDate('2020-01-01') SETTINGS optimize_substitute_columns = 0;
SELECT 'tuple filter', count() FROM t_sub_tuple WHERE toDate(dt.1) = toDate('2020-01-01') SETTINGS optimize_substitute_columns = 1;
SELECT 'tuple projection', toString(dt) FROM t_sub_tuple WHERE k > tuple(0) SETTINGS optimize_substitute_columns = 0;
SELECT 'tuple projection', toString(dt) FROM t_sub_tuple WHERE k > tuple(0) SETTINGS optimize_substitute_columns = 1;

DROP TABLE t_sub_datetime;
DROP TABLE t_sub_datetime64;
DROP TABLE t_sub_tuple;
