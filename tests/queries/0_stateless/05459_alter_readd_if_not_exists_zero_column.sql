-- A name freed earlier in the same ALTER must be visible to ADD COLUMN IF NOT EXISTS.

DROP TABLE IF EXISTS t_readd_after_drop;
CREATE TABLE t_readd_after_drop (x UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_readd_after_drop DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_drop;
DROP TABLE t_readd_after_drop;

DROP TABLE IF EXISTS t_readd_after_rename;
CREATE TABLE t_readd_after_rename (x UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_readd_after_rename RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_rename;
DROP TABLE t_readd_after_rename;

DROP TABLE IF EXISTS t_conditional_after_readd;
CREATE TABLE t_conditional_after_readd (id UInt8, x UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_conditional_after_readd
    DROP COLUMN x,
    ADD COLUMN IF NOT EXISTS x UInt8,
    RENAME COLUMN IF EXISTS x TO y;
DESC TABLE t_conditional_after_readd;
DROP TABLE t_conditional_after_readd;
