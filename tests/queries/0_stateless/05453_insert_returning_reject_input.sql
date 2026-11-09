SET async_insert = 0;

CREATE TABLE t_returning_input (x UInt64) ENGINE = Memory;

INSERT INTO t_returning_input RETURNING (SELECT * FROM input('y UInt64')) VALUES (1); -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

INSERT INTO t_returning_input RETURNING (SELECT (SELECT sum(y) FROM input('y UInt64'))) SELECT 1; -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

INSERT INTO t_returning_input RETURNING (WITH src AS (SELECT * FROM input('y UInt64')) SELECT * FROM src) VALUES (1); -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

-- `input` behind a view is not in the query text, so it is found when the insert has already read the input.
-- The insert reads the input only when the client sends the data separately, not when the server parses it.
SET send_table_structure_on_insert_with_inline_data = 1;
CREATE VIEW v_returning_input AS SELECT * FROM input('y UInt64');
INSERT INTO t_returning_input RETURNING (SELECT * FROM v_returning_input) VALUES (1); -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

CREATE VIEW pv_returning_input AS SELECT * FROM input('y UInt64') WHERE y > {p:UInt64};
INSERT INTO t_returning_input RETURNING (SELECT * FROM pv_returning_input(p = 0)) VALUES (1); -- { serverError INVALID_USAGE_OF_INPUT }
SELECT count() FROM t_returning_input;

INSERT INTO t_returning_input RETURNING (SELECT sum(x) FROM t_returning_input)
SELECT x FROM input('x UInt64') FORMAT Values (2), (3);
SELECT count(), sum(x) FROM t_returning_input;

DROP VIEW pv_returning_input;
DROP VIEW v_returning_input;
DROP TABLE t_returning_input;
