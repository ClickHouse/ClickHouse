-- A quoted identifier with the `<column>.` prefix in a `DEFAULT` written inside a tuple element is a
-- self-reference only if it names an actual subcolumn of the column. Otherwise it refers to an
-- independent column with a dotted name, which is a valid dependency.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_dotted;

CREATE TABLE t_tuple_default_dotted (id UInt8, `c.a` UInt8, c Tuple(b UInt8 DEFAULT `c.a`)) ENGINE = Memory;
INSERT INTO t_tuple_default_dotted (id, `c.a`) VALUES (1, 7);
SELECT c FROM t_tuple_default_dotted;
DROP TABLE t_tuple_default_dotted;

-- An exact column name takes precedence over a subcolumn, so `c.a` refers to the independent column
-- even if the tuple `c` has an element `a`.
CREATE TABLE t_tuple_default_dotted (id UInt8, `c.a` UInt8, c Tuple(a UInt8, b UInt8 DEFAULT `c.a`)) ENGINE = Memory;
INSERT INTO t_tuple_default_dotted (id, `c.a`) VALUES (1, 7);
SELECT c FROM t_tuple_default_dotted;
DROP TABLE t_tuple_default_dotted;

CREATE TABLE t_tuple_default_dotted (id UInt8, `c.a` UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_dotted MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`);
INSERT INTO t_tuple_default_dotted (id, `c.a`) VALUES (1, 8);
SELECT c FROM t_tuple_default_dotted;
DROP TABLE t_tuple_default_dotted;

-- An actual subcolumn is still rejected, including the subcolumns of a nested tuple.
CREATE TABLE t_tuple_default_dotted (id UInt8, c Tuple(a UInt8, b UInt8 DEFAULT `c.a`)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_tuple_default_dotted (id UInt8, c Tuple(x Tuple(a UInt8), b UInt8 DEFAULT `c.x.a`)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_tuple_default_dotted (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_dotted MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_tuple_default_dotted;
