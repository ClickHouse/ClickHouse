SET async_insert = 0;

CREATE TABLE t_returning_scalar_input (x UInt64) ENGINE = Memory;

INSERT INTO t_returning_scalar_input RETURNING
    (WITH (v -> v + 1) AS input SELECT input(x) FROM t_returning_scalar_input)
SELECT 1;
SELECT x FROM t_returning_scalar_input;

TRUNCATE TABLE t_returning_scalar_input;
INSERT INTO t_returning_scalar_input
WITH (v -> v + 1) AS input SELECT input(10);
SELECT x FROM t_returning_scalar_input;

TRUNCATE TABLE t_returning_scalar_input;
INSERT INTO t_returning_scalar_input RETURNING
    (WITH (v -> v + 1) AS input SELECT input(x) + input(x) FROM t_returning_scalar_input)
SELECT 2;
SELECT x FROM t_returning_scalar_input;

INSERT INTO t_returning_scalar_input RETURNING
    (WITH (v -> v + 1) AS input SELECT input(y) FROM input('y UInt64'))
SELECT 3; -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count(), sum(x) FROM t_returning_scalar_input;

DROP TABLE t_returning_scalar_input;
