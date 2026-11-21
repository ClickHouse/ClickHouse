-- `AlterCommands::prepare` ignores `RENAME COLUMN IF EXISTS` and `DROP COLUMN IF EXISTS` when the column did not exist
-- before the query, even if a preceding command of the same query adds it. The columns that tell an independent
-- dotted column from a subcolumn of the column itself, for a reference in a `DEFAULT` written inside a tuple
-- element, must follow the same decisions. See https://github.com/ClickHouse/ClickHouse/issues/2797.

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_tuple_default_skipped_conditional;

-- The rename is ignored, so there is no column `c.a` and the reference is to the element `a` of `c` itself.
CREATE TABLE t_tuple_default_skipped_conditional (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_skipped_conditional
    ADD COLUMN `c.z` UInt8,
    RENAME COLUMN IF EXISTS `c.z` TO `c.a`,
    MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_skipped_conditional
    ADD COLUMN `c.z` UInt8,
    MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`),
    RENAME COLUMN IF EXISTS `c.z` TO `c.a`; -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_skipped_conditional' ORDER BY position;
DROP TABLE t_tuple_default_skipped_conditional;

-- The drop is ignored, so the added column `c.a` stays and the reference is to it.
CREATE TABLE t_tuple_default_skipped_conditional (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;
ALTER TABLE t_tuple_default_skipped_conditional
    ADD COLUMN `c.a` UInt8,
    DROP COLUMN IF EXISTS `c.a`,
    MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`);
INSERT INTO t_tuple_default_skipped_conditional (id, `c.a`) VALUES (1, 7);
SELECT c FROM t_tuple_default_skipped_conditional;
DROP TABLE t_tuple_default_skipped_conditional;

-- `DROP COLUMN IF EXISTS` of a flattened `Nested` group is skipped, because there is no column with exactly this name.
CREATE TABLE t_tuple_default_skipped_conditional
(
    id UInt8,
    `c.n` Nested(a UInt8),
    c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8))
) ENGINE = Memory;
ALTER TABLE t_tuple_default_skipped_conditional
    DROP COLUMN IF EXISTS `c.n`,
    MODIFY COLUMN c Tuple(n Tuple(a Array(UInt8)), b Array(UInt8) DEFAULT `c.n.a`);
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_tuple_default_skipped_conditional' ORDER BY position;
INSERT INTO t_tuple_default_skipped_conditional (id, `c.n.a`) VALUES (1, [9]);
SELECT c FROM t_tuple_default_skipped_conditional;
DROP TABLE t_tuple_default_skipped_conditional;
