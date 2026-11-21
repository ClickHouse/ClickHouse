-- A reference `c.n.a` or `c.a` in a `DEFAULT` written inside a tuple element of the column `c` refers to an
-- independent column if the table has a column named exactly so after the query: this takes into account
-- the flattening of `Nested` columns, the removal of a whole `Nested` group and the renames in the same `ALTER`.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_tuple_default_nested;

-- A flattened `Nested` column is an independent column.
CREATE TABLE t_tuple_default_nested
(
    id UInt8,
    `c.n` Nested(a UInt8),
    c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`)
) ENGINE = Memory;
INSERT INTO t_tuple_default_nested (id, `c.n.a`) VALUES (1, [7]);
SELECT c FROM t_tuple_default_nested;
DROP TABLE t_tuple_default_nested;

CREATE TABLE t_tuple_default_nested (id UInt8, c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8))) ENGINE = Memory;
ALTER TABLE t_tuple_default_nested
    ADD COLUMN `c.n` Nested(a UInt8),
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`);
INSERT INTO t_tuple_default_nested (id, `c.n.a`) VALUES (1, [8]);
SELECT c FROM t_tuple_default_nested;
DROP TABLE t_tuple_default_nested;

-- Dropping the whole `Nested` group removes its flattened columns.
CREATE TABLE t_tuple_default_nested
(
    id UInt8,
    `c.n` Nested(a UInt8),
    c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8))
) ENGINE = Memory;
ALTER TABLE t_tuple_default_nested
    DROP COLUMN `c.n`,
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_nested
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`),
    DROP COLUMN `c.n`; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_tuple_default_nested;

-- A rename after the command rewrites the reference to the new name of the column.
CREATE TABLE t_tuple_default_nested (id UInt8, `c.a` UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_nested
    MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`),
    RENAME COLUMN `c.a` TO `c.z`;
SELECT name, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_nested' ORDER BY position;
INSERT INTO t_tuple_default_nested (id, `c.z`) VALUES (1, 7);
SELECT c FROM t_tuple_default_nested;
DROP TABLE t_tuple_default_nested;
