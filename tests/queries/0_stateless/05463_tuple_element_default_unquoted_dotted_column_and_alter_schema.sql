-- A reference `c.a` in a `DEFAULT` written inside a tuple element of the column `c` is a self-reference
-- only if `a` is a subcolumn of `c` and the resulting table schema has no column named exactly `c.a`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_unquoted;

-- The unquoted spelling refers to the independent column `c.a` just like the quoted one.
CREATE TABLE t_tuple_default_unquoted (id UInt8, `c.a` UInt8, c Tuple(a UInt8, b UInt8 DEFAULT c.a)) ENGINE = Memory;
INSERT INTO t_tuple_default_unquoted (id, `c.a`) VALUES (1, 7);
SELECT c FROM t_tuple_default_unquoted;
DROP TABLE t_tuple_default_unquoted;

CREATE TABLE t_tuple_default_unquoted (id UInt8, `c.a` UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_unquoted MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT c.a);
INSERT INTO t_tuple_default_unquoted (id, `c.a`) VALUES (1, 8);
SELECT c FROM t_tuple_default_unquoted;
DROP TABLE t_tuple_default_unquoted;

-- Without the independent column the unquoted reference is still a self-reference.
CREATE TABLE t_tuple_default_unquoted (id UInt8, c Tuple(a UInt8, b UInt8 DEFAULT c.a)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- A column dropped by the same `ALTER` does not shadow the subcolumn.
CREATE TABLE t_tuple_default_unquoted (id UInt8, `c.a` UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_unquoted DROP COLUMN `c.a`, MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_unquoted MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT c.a), DROP COLUMN `c.a`; -- { serverError BAD_ARGUMENTS }
SELECT name, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_unquoted' ORDER BY position;
DROP TABLE t_tuple_default_unquoted;

