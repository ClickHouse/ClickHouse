SET flatten_nested = 1, mutations_sync = 2, alter_sync = 2;

-- `ADD COLUMN n.c` appends `n.c` after `x`; `DROP COLUMN n` drops it with the rest of the group.
CREATE TABLE t_wide (p UInt8, n Nested(a UInt32, b UInt32), x UInt8) ENGINE = MergeTree ORDER BY p
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
ALTER TABLE t_wide ADD COLUMN n.c Array(UInt32);
INSERT INTO t_wide (p, n.a, n.b, x, n.c) VALUES (1, [1, 2], [3, 4], 9, [5, 6]);
ALTER TABLE t_wide DROP COLUMN n;
SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_wide' ORDER BY position);
SELECT groupArray(column) FROM (SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_wide' AND active AND NOT startsWith(column, '_') ORDER BY column_position);
SELECT * FROM t_wide;

-- the same on a compact part
CREATE TABLE t_compact (p UInt8, n Nested(a UInt32, b UInt32), x UInt8) ENGINE = MergeTree ORDER BY p
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;
ALTER TABLE t_compact ADD COLUMN n.c Array(UInt32);
INSERT INTO t_compact (p, n.a, n.b, x, n.c) VALUES (1, [1, 2], [3, 4], 9, [5, 6]);
ALTER TABLE t_compact DROP COLUMN n;
SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_compact' ORDER BY position);
SELECT groupArray(column) FROM (SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_compact' AND active AND NOT startsWith(column, '_') ORDER BY column_position);
SELECT * FROM t_compact;

-- `CLEAR COLUMN n` on a compact part clears `n.c` too, so the group reads as empty arrays
CREATE TABLE t_clear (p UInt8, n Nested(a UInt32, b UInt32), x UInt8) ENGINE = MergeTree ORDER BY p
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;
ALTER TABLE t_clear ADD COLUMN n.c Array(UInt32);
INSERT INTO t_clear (p, n.a, n.b, x, n.c) VALUES (1, [1, 2], [3, 4], 9, [5, 6]);
ALTER TABLE t_clear CLEAR COLUMN n;
SELECT * FROM t_clear;

-- members declared apart in CREATE (#37258)
CREATE TABLE t_create (`n.a` Array(UInt32), id UInt32, `n.b` Array(UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_create VALUES ([1], 1, [2]);
ALTER TABLE t_create DROP COLUMN n;
SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_create' ORDER BY position);
SELECT * FROM t_create;

-- Memory engine
CREATE TABLE t_memory (p UInt8, n Nested(a UInt32, b UInt32), x UInt8) ENGINE = Memory;
ALTER TABLE t_memory ADD COLUMN n.c Array(UInt32);
INSERT INTO t_memory (p, n.a, n.b, x, n.c) VALUES (1, [1, 2], [3, 4], 9, [5, 6]);
ALTER TABLE t_memory DROP COLUMN n;
SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_memory' ORDER BY position);
SELECT * FROM t_memory;

-- a sorting key member declared apart blocks the group drop
CREATE TABLE t_key (p UInt8, n Nested(a UInt32, b UInt32), x UInt8) ENGINE = MergeTree ORDER BY p;
ALTER TABLE t_key ADD COLUMN n.c Array(UInt32), MODIFY ORDER BY (p, n.c);
ALTER TABLE t_key DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- a MATERIALIZED column reading a member declared apart blocks the group drop
CREATE TABLE t_dep (p UInt8, n Nested(a UInt32, b UInt32), x UInt8) ENGINE = MergeTree ORDER BY p;
ALTER TABLE t_dep ADD COLUMN n.c Array(UInt32);
ALTER TABLE t_dep ADD COLUMN m UInt64 MATERIALIZED length(n.c);
ALTER TABLE t_dep DROP COLUMN n; -- { serverError UNKNOWN_IDENTIFIER }

-- the group's first column is an ALIAS: the drop still runs the mutation, so re-adding `n.b` reads no old data
CREATE TABLE t_alias (p UInt8, `n.a` Array(UInt32) ALIAS [1], x UInt8) ENGINE = MergeTree ORDER BY p
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
ALTER TABLE t_alias ADD COLUMN n.b Array(UInt32);
INSERT INTO t_alias (p, x, n.b) VALUES (1, 9, [5, 6]);
ALTER TABLE t_alias DROP COLUMN n;
ALTER TABLE t_alias ADD COLUMN n.b Array(UInt32);
SELECT * FROM t_alias;

-- a non-array `n.s` declared apart is not part of the Nested structure and stays
CREATE TABLE t_scalar (p UInt8, n Nested(a UInt32, b UInt32), x UInt8) ENGINE = MergeTree ORDER BY p;
ALTER TABLE t_scalar ADD COLUMN n.s UInt8;
ALTER TABLE t_scalar DROP COLUMN n;
SELECT groupArray(name) FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_scalar' ORDER BY position);

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_clear;
DROP TABLE t_create;
DROP TABLE t_memory;
DROP TABLE t_key;
DROP TABLE t_dep;
DROP TABLE t_alias;
DROP TABLE t_scalar;
