-- `ADD COLUMN IF NOT EXISTS` must see a name freed by an earlier command of the same ALTER (#117824).

DROP TABLE IF EXISTS t_readd_after_drop;
CREATE TABLE t_readd_after_drop (x UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_readd_after_drop DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_drop;
DROP TABLE t_readd_after_drop;

DROP TABLE IF EXISTS t_readd_after_rename;
CREATE TABLE t_readd_after_rename (x UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_readd_after_rename VALUES (1);
ALTER TABLE t_readd_after_rename RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_rename;
SELECT count(), min(x_old), max(x_old) FROM t_readd_after_rename;
DROP TABLE t_readd_after_rename;

DROP TABLE IF EXISTS t_readd_nested;
CREATE TABLE t_readd_nested (n Nested(a UInt8), id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_nested DROP COLUMN n, ADD COLUMN IF NOT EXISTS n.a UInt8;
DESC TABLE t_readd_nested;
DROP TABLE t_readd_nested;

-- `DROP COLUMN IF EXISTS n` does not remove dotted children, so the following rename of `n.a` still runs.
DROP TABLE IF EXISTS t_readd_nested_skip;
CREATE TABLE t_readd_nested_skip (`n.a` UInt8, id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_nested_skip DROP COLUMN IF EXISTS n, RENAME COLUMN IF EXISTS `n.a` TO m;
DESC TABLE t_readd_nested_skip;
DROP TABLE t_readd_nested_skip;
