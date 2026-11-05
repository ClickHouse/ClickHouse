-- `ADD COLUMN IF NOT EXISTS` that is a no-op does not add columns that a reference in a `DEFAULT` written
-- inside a tuple element could refer to: such a reference is a reference to a subcolumn of the column itself.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_tuple_default_skipped_add;

-- The `Nested` group `c.n` exists, so adding it again is a no-op and the column `c.n.a` is not added.
CREATE TABLE t_tuple_default_skipped_add
(
    id UInt8,
    `c.n` Nested(z UInt8),
    c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8))
) ENGINE = Memory;
ALTER TABLE t_tuple_default_skipped_add
    ADD COLUMN IF NOT EXISTS `c.n` Nested(a UInt8),
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_skipped_add
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`),
    ADD COLUMN IF NOT EXISTS `c.n` Nested(a UInt8); -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_skipped_add' ORDER BY position;
DROP TABLE t_tuple_default_skipped_add;

-- The same with an existing scalar column `c.n`.
CREATE TABLE t_tuple_default_skipped_add (id UInt8, `c.n` UInt8, c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8))) ENGINE = Memory;
ALTER TABLE t_tuple_default_skipped_add
    ADD COLUMN IF NOT EXISTS `c.n` Nested(a UInt8),
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_tuple_default_skipped_add;

-- An add of a new `Nested` group with `IF NOT EXISTS` still makes its flattened columns available.
CREATE TABLE t_tuple_default_skipped_add (id UInt8, c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8))) ENGINE = Memory;
ALTER TABLE t_tuple_default_skipped_add
    ADD COLUMN IF NOT EXISTS `c.n` Nested(a UInt8),
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`);
INSERT INTO t_tuple_default_skipped_add (id, `c.n.a`) VALUES (1, [9]);
SELECT c FROM t_tuple_default_skipped_add;
DROP TABLE t_tuple_default_skipped_add;
