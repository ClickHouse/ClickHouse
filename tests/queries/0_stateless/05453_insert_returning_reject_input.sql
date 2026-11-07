SET async_insert = 0;

CREATE TABLE t_returning_input (x UInt64) ENGINE = Memory;

INSERT INTO t_returning_input RETURNING (SELECT * FROM input('y UInt64')) VALUES (1); -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

INSERT INTO t_returning_input RETURNING (SELECT (SELECT sum(y) FROM input('y UInt64'))) SELECT 1; -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

INSERT INTO t_returning_input RETURNING (WITH src AS (SELECT * FROM input('y UInt64')) SELECT * FROM src) VALUES (1); -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

INSERT INTO t_returning_input RETURNING (SELECT sum(x) FROM t_returning_input)
SELECT x FROM input('x UInt64') FORMAT Values (2), (3);
SELECT count(), sum(x) FROM t_returning_input;

DROP TABLE t_returning_input;
