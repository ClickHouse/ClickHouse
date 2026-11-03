-- `RENAME COLUMN IF EXISTS` of an absent column is a no-op, so its target must not count as an independent
-- column that a reference in a `DEFAULT` inside a tuple element could resolve to instead of a subcolumn.
-- See https://github.com/ClickHouse/ClickHouse/issues/2797.

DROP TABLE IF EXISTS t_tuple_default_skipped_rename;
CREATE TABLE t_tuple_default_skipped_rename (id UInt8, c Tuple(a UInt8, b UInt8)) ENGINE = Memory;

ALTER TABLE t_tuple_default_skipped_rename RENAME COLUMN IF EXISTS `c.z` TO `c.a`, MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_tuple_default_skipped_rename MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`), RENAME COLUMN IF EXISTS `c.z` TO `c.a`; -- { serverError BAD_ARGUMENTS }

-- A rename that actually happens still provides the independent column.
ALTER TABLE t_tuple_default_skipped_rename ADD COLUMN `c.z` UInt8;
ALTER TABLE t_tuple_default_skipped_rename RENAME COLUMN IF EXISTS `c.z` TO `c.a`, MODIFY COLUMN c Tuple(a UInt8, b UInt8 DEFAULT `c.a`);
INSERT INTO t_tuple_default_skipped_rename (id, `c.a`) VALUES (1, 5);
SELECT c FROM t_tuple_default_skipped_rename;

DROP TABLE t_tuple_default_skipped_rename;
