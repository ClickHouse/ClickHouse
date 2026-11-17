-- A table created from `null` without a structure argument has the columns it was created with. The table function used
-- `dummy UInt8` instead, so reading a column of the table failed with `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP TABLE IF EXISTS t_null;

CREATE TABLE t_null (a String, b UInt8) AS null();
SELECT count() FROM (SELECT a, b FROM t_null);
INSERT INTO t_null VALUES ('x', 1);
SELECT count() FROM (SELECT a, b FROM t_null);

DETACH TABLE t_null PERMANENTLY;
ATTACH TABLE t_null;
SELECT count() FROM (SELECT a, b FROM t_null);

DROP TABLE t_null;
