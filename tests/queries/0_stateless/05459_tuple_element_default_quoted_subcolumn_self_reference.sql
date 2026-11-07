-- A `DEFAULT` expression written inside a tuple element must not reference the column it belongs to
-- through the quoted subcolumn spelling either: `` `c.a` `` is a single identifier part, so the check
-- of the root of a compound identifier did not catch it, and the table was created with a
-- self-referential default that failed only on an insert omitting the column.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_quoted_self;
DROP TABLE IF EXISTS t_tuple_default_quoted_other;

CREATE TABLE t_tuple_default_quoted_self (id UInt8, c Tuple(a UInt8, b UInt8 DEFAULT `c.a`)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_tuple_default_quoted_self (id UInt8) ENGINE = Memory;
ALTER TABLE t_tuple_default_quoted_self ADD COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_quoted_self ADD COLUMN c Tuple(a UInt8, b UInt8);
ALTER TABLE t_tuple_default_quoted_self MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`); -- { serverError BAD_ARGUMENTS }

-- A quoted reference to a subcolumn of another column is allowed.
CREATE TABLE t_tuple_default_quoted_other (other Tuple(a UInt8), c Tuple(a UInt8, b UInt8 DEFAULT `other.a`)) ENGINE = Memory;
INSERT INTO t_tuple_default_quoted_other (other) VALUES ((3));
SELECT c FROM t_tuple_default_quoted_other;

DROP TABLE t_tuple_default_quoted_self;
DROP TABLE t_tuple_default_quoted_other;
