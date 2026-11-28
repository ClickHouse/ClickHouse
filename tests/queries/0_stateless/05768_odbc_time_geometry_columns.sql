-- Tags: no-fasttest

CREATE TABLE t_odbc_time (id Int32, t Nullable(Time)) ENGINE = ODBC('DSN=none', 'db', 't');
INSERT INTO t_odbc_time SELECT 1, toTime('12:34:56'); -- { serverError UNKNOWN_TYPE }
DROP TABLE t_odbc_time;

CREATE TABLE t_odbc_geometry (id Int32, g Geometry) ENGINE = ODBC('DSN=none', 'db', 't');
INSERT INTO t_odbc_geometry (id) SELECT 1; -- { serverError UNKNOWN_TYPE }
DROP TABLE t_odbc_geometry;
