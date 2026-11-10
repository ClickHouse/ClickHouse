-- `Nested::collect` drops a non-flattened Nested whose name prefix is a flattened Nested group, but its streams are still
-- written, so they are checked for file name collisions too.
SET flatten_nested = 0;
DROP TABLE IF EXISTS t_collision;

-- The element `x` of `n.a` and the column `n.a.x` are written into one file.
CREATE TABLE t_collision (`n.a` Nested(x UInt64), `n.b` Array(UInt64), `n.a.x` UInt64) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- `n.a` shares the array sizes file `n.size0` with the group `n`, as `n Nested(a UInt64)` next to `n.b` does.
CREATE TABLE t_collision (`n.a` Nested(x UInt64), `n.b` Array(UInt64)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_collision (`n.a` Nested(x UInt64)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_collision ADD COLUMN `n.b` Array(UInt64); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_collision;

-- Without shared offsets the streams of `n.a` and `n.b` are independent.
CREATE TABLE t_collision (`n.a` Nested(x UInt64), `n.b` Array(UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_collision VALUES ([(1), (2)], [3]);
SELECT * FROM t_collision;
DROP TABLE t_collision;

-- A group with another prefix is not affected.
CREATE TABLE t_collision (`n.a` Nested(x UInt64), `m.b` Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;
