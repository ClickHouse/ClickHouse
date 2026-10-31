-- After a `DROP COLUMN` or `RENAME COLUMN` frees a name in the same ALTER statement,
-- `ADD COLUMN IF NOT EXISTS` must see the freed name and add the column (issue #117824).

-- 1. Re-add right after DROP (the issue's scenario): x must survive.
DROP TABLE IF EXISTS t_readd_after_drop;
CREATE TABLE t_readd_after_drop (x UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_readd_after_drop DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_drop;
DROP TABLE t_readd_after_drop;

-- Same statement without IF NOT EXISTS: identical outcome, proves only the flag's path differed.
DROP TABLE IF EXISTS t_readd_no_flag;
CREATE TABLE t_readd_no_flag (x UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_readd_no_flag DROP COLUMN x, ADD COLUMN x UInt8;
DESC TABLE t_readd_no_flag;
DROP TABLE t_readd_no_flag;

-- 2. Archive-and-recreate: rename the old column away, then re-add the original name.
DROP TABLE IF EXISTS t_readd_after_rename;
CREATE TABLE t_readd_after_rename (x UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_readd_after_rename VALUES (1);
ALTER TABLE t_readd_after_rename RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_rename;
SELECT count(), min(x_old), max(x_old) FROM t_readd_after_rename;
DROP TABLE t_readd_after_rename;

-- 3. Dropping a whole Nested group frees its subnames: a follow-up subcolumn add must work.
DROP TABLE IF EXISTS t_readd_nested;
CREATE TABLE t_readd_nested (n Nested(a UInt8), id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_nested DROP COLUMN n, ADD COLUMN IF NOT EXISTS n.a UInt8;
DESC TABLE t_readd_nested;
DROP TABLE t_readd_nested;

-- Mirror: ADD expands the group first, then dropping the group name removes the added child too.
DROP TABLE IF EXISTS t_readd_nested_mirror;
CREATE TABLE t_readd_nested_mirror (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_nested_mirror ADD COLUMN IF NOT EXISTS n.a UInt8, DROP COLUMN IF EXISTS n;
DESC TABLE t_readd_nested_mirror;
DROP TABLE t_readd_nested_mirror;

-- 4. Reverse order: ADD then DROP in one statement must still end without the column.
-- (A second column keeps the table non-empty; dropping ALL columns is rejected separately.)
DROP TABLE IF EXISTS t_readd_reverse;
CREATE TABLE t_readd_reverse (x UInt8, id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_reverse ADD COLUMN IF NOT EXISTS x UInt8, DROP COLUMN x;
DESC TABLE t_readd_reverse;
DROP TABLE t_readd_reverse;

-- 5. `DROP COLUMN IF EXISTS` on an absent column is skipped, so the follow-up add creates it.
DROP TABLE IF EXISTS t_readd_ghost;
CREATE TABLE t_readd_ghost (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_ghost DROP COLUMN IF EXISTS ghost, ADD COLUMN IF NOT EXISTS ghost UInt8;
DESC TABLE t_readd_ghost;
DROP TABLE t_readd_ghost;

-- 6. Command chain: rename away, re-add, then drop the archived copy.
DROP TABLE IF EXISTS t_readd_chain;
CREATE TABLE t_readd_chain (x UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_readd_chain RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt8, DROP COLUMN x_old;
DESC TABLE t_readd_chain;
DROP TABLE t_readd_chain;

-- 7. Flatten-nested off: `n.a` stays dotted in the snapshot, the re-add must still be seen.
DROP TABLE IF EXISTS t_readd_flatten_off;
SET flatten_nested = 0;
CREATE TABLE t_readd_flatten_off (id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_flatten_off DROP COLUMN IF EXISTS n.a, ADD COLUMN IF NOT EXISTS n.a UInt8;
DESC TABLE t_readd_flatten_off;
DROP TABLE t_readd_flatten_off;
SET flatten_nested = 1;
