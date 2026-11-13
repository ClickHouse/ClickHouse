-- A reference `c.n.x` in a `DEFAULT` written inside a tuple element of the column `c` is not a self-reference
-- if it is a subcolumn of another column with a dotted name (`c.n`), that `c` does not have.

DROP TABLE IF EXISTS t_tuple_default_dotted_subcolumn;

CREATE TABLE t_tuple_default_dotted_subcolumn
(
    id UInt8,
    `c.n` Tuple(x UInt8),
    `c.a` Array(UInt8),
    c Tuple(b UInt8 DEFAULT `c.n.x`, s UInt64 DEFAULT c.a.size0)
) ENGINE = Memory;
INSERT INTO t_tuple_default_dotted_subcolumn (id, `c.n`, `c.a`) VALUES (1, tuple(7), [1, 2, 3]);
SELECT id, c FROM t_tuple_default_dotted_subcolumn;
DROP TABLE t_tuple_default_dotted_subcolumn;

-- Still a self-reference if the column being defined owns the name.
CREATE TABLE t_tuple_default_dotted_subcolumn (id UInt8, `c.n` Tuple(x UInt8), c Tuple(b UInt8 DEFAULT `c.m.x`, m Tuple(x UInt8))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
-- Ambiguous: both `c.n` and `c` have the subcolumn `c.n.x`.
CREATE TABLE t_tuple_default_dotted_subcolumn (id UInt8, `c.n` Tuple(x UInt8), c Tuple(b UInt8 DEFAULT `c.n.x`, n Tuple(x UInt8))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- The same with `ALTER`.
CREATE TABLE t_tuple_default_dotted_subcolumn (id UInt8, `c.n` Tuple(x UInt8), `c.a` Array(UInt8), c Tuple(b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_dotted_subcolumn MODIFY COLUMN c Tuple(b UInt8 DEFAULT c.m.x, m Tuple(x UInt8)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_dotted_subcolumn MODIFY COLUMN c Tuple(b UInt8 DEFAULT c.n.x, n Tuple(x UInt8)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_dotted_subcolumn MODIFY COLUMN c Tuple(b UInt8 DEFAULT `c.n.x`, s UInt64 DEFAULT c.a.size0);
INSERT INTO t_tuple_default_dotted_subcolumn (id, `c.n`, `c.a`) VALUES (2, tuple(9), [4, 5]);
-- A column added in the same query owns the subcolumn as well.
ALTER TABLE t_tuple_default_dotted_subcolumn ADD COLUMN `c.m` Tuple(x UInt8), MODIFY COLUMN c Tuple(b UInt8 DEFAULT `c.m.x`, s UInt64 DEFAULT 0);
INSERT INTO t_tuple_default_dotted_subcolumn (id, `c.n`, `c.a`, `c.m`) VALUES (3, tuple(1), [], tuple(4));
SELECT id, c FROM t_tuple_default_dotted_subcolumn ORDER BY id;
DROP TABLE t_tuple_default_dotted_subcolumn;
