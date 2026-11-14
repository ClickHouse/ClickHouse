-- Members of a Nested column share one array sizes stream on disk. A mutation that writes a member with other array
-- sizes than the stored members have must fail instead of corrupting them.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t6;
DROP TABLE IF EXISTS t7;
DROP TABLE IF EXISTS t8;
DROP TABLE IF EXISTS t9;
DROP TABLE IF EXISTS t10;
DROP TABLE IF EXISTS t11;
DROP TABLE IF EXISTS t12;
DROP TABLE IF EXISTS t13;
DROP TABLE IF EXISTS t14;
DROP TABLE IF EXISTS t15;

SELECT 'materialize a default of other sizes';
CREATE TABLE t1 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t1 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']);
ALTER TABLE t1 ADD COLUMN `n.x` Array(Nullable(String)) DEFAULT emptyArrayString();
ALTER TABLE t1 MATERIALIZE COLUMN `n.x` SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't1' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't1' SYNC FORMAT Null;
SELECT id, n.a, n.c FROM t1 ORDER BY id;

SELECT 'compact part';
CREATE TABLE t2 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '10G';
INSERT INTO t2 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']);
ALTER TABLE t2 ADD COLUMN `n.x` Array(UInt64) DEFAULT [7, 7, 7] AFTER id;
ALTER TABLE t2 MATERIALIZE COLUMN `n.x` SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't2' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't2' SYNC FORMAT Null;
SELECT id, n.a, n.c FROM t2 ORDER BY id;

SELECT 'stored MATERIALIZED member';
CREATE TABLE t3 (id UInt64, n Nested(a UInt64, c String), `n.m` Array(UInt64) MATERIALIZED arrayMap(v -> v + 100, `n.a`))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t3 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']);
ALTER TABLE t3 MODIFY COLUMN `n.m` Array(UInt64) MATERIALIZED [7, 7, 7];
ALTER TABLE t3 MATERIALIZE COLUMN `n.m` SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't3' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't3' SYNC FORMAT Null;
SELECT id, n.a, n.c, n.m FROM t3 ORDER BY id;

SELECT 'update a member the part does not store';
CREATE TABLE t4 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t4 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']);
ALTER TABLE t4 ADD COLUMN `n.x` Array(UInt64) DEFAULT [7, 7, 7];
ALTER TABLE t4 UPDATE `n.x` = [9, 9, 9] WHERE 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't4' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't4' SYNC FORMAT Null;
SELECT id, n.a, n.c FROM t4 ORDER BY id;

SELECT 'update a member the part does not store with the right sizes';
CREATE TABLE t5 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t5 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']);
ALTER TABLE t5 ADD COLUMN `n.x` Array(UInt64) DEFAULT [7, 7, 7];
ALTER TABLE t5 UPDATE `n.x` = arrayMap(v -> v * 10, `n.a`) WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.x FROM t5 ORDER BY id;

SELECT 'update every member of some rows';
CREATE TABLE t6 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t6 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']);
ALTER TABLE t6 ADD COLUMN `n.x` Array(UInt64) DEFAULT [7, 7, 7];
ALTER TABLE t6 UPDATE `n.a` = [5], `n.c` = ['s'], `n.x` = [6] WHERE id = 0 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't6' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't6' SYNC FORMAT Null;
SELECT id, n.a, n.c FROM t6 ORDER BY id;

SELECT 'delete rewriting the whole part';
CREATE TABLE t7 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t7 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']), (2, [4, 5, 6], ['s', 't', 'u']);
ALTER TABLE t7 ADD COLUMN `n.x` Array(UInt64) DEFAULT [7, 7, 7] AFTER id;
ALTER TABLE t7 DELETE WHERE id = 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't7' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't7' SYNC FORMAT Null;
SELECT id, n.a, n.c, n.x FROM t7 ORDER BY id;

SELECT 'consistent mutations';
CREATE TABLE t8 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t8 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']), (2, [4, 5, 6], ['s', 't', 'u']);
ALTER TABLE t8 ADD COLUMN `n.y` Array(UInt64) DEFAULT arrayMap(v -> v + 1, `n.a`);
ALTER TABLE t8 MATERIALIZE COLUMN `n.y` SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.y FROM t8 ORDER BY id;
ALTER TABLE t8 DELETE WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.y FROM t8 ORDER BY id;
ALTER TABLE t8 ADD COLUMN `n.z` Array(UInt64) DEFAULT [7, 7, 7];
ALTER TABLE t8 MATERIALIZE COLUMN `n.z`, UPDATE `n.a` = [1], `n.c` = ['z'], `n.y` = [1], `n.z` = [2] WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.y, n.z FROM t8 ORDER BY id;

SELECT 'apply patches';
CREATE TABLE t9 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t9 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']);
ALTER TABLE t9 ADD COLUMN `n.x` Array(UInt64) DEFAULT [7, 7, 7] AFTER id;
SET enable_lightweight_update = 1;
UPDATE t9 SET `n.x` = [9, 9, 9] WHERE 1;
ALTER TABLE t9 APPLY PATCHES SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't9' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't9' SYNC FORMAT Null;
SELECT id, n.a, n.c FROM t9 ORDER BY id;

SELECT 'group the part does not store';
CREATE TABLE t10 (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t10 VALUES (0), (1), (2);
ALTER TABLE t10 ADD COLUMN `n.a` Array(UInt64) DEFAULT [1], ADD COLUMN `n.b` Array(UInt64) DEFAULT [1, 2];
ALTER TABLE t10 DELETE WHERE id = 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't10' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't10' SYNC FORMAT Null;
SELECT id, n.a, n.b FROM t10 ORDER BY id;

SELECT 'whole-part rewrite of consistent members';
CREATE TABLE t11 (id UInt64, n Nested(a UInt64, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t11 VALUES (0, [1, 2], ['p', 'q']), (1, [3], ['r']), (2, [4, 5, 6], ['s', 't', 'u']);
ALTER TABLE t11 ADD COLUMN `n.w` Array(UInt64), ADD COLUMN `n.y` Array(UInt64) DEFAULT arrayMap(v -> v + 1, `n.a`);
ALTER TABLE t11 DELETE WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.w, n.y FROM t11 ORDER BY id;
ALTER TABLE t11 ADD COLUMN `n.z` Array(UInt64) DEFAULT arrayMap(v -> v * 10, `n.a`);
ALTER TABLE t11 DELETE WHERE id = 2, MATERIALIZE COLUMN `n.z` SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.w, n.y, n.z FROM t11 ORDER BY id;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't11' AND active AND endsWith(column, '.size0');

SELECT 'recompute a MATERIALIZED member to other sizes';
CREATE TABLE t12 (id UInt64, k UInt64, n Nested(a UInt64, c String), `n.m` Array(UInt64) MATERIALIZED range(k))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t12 (id, k, `n.a`, `n.c`) VALUES (0, 2, [1, 2], ['p', 'q']), (1, 1, [3], ['r']);
ALTER TABLE t12 UPDATE k = 3 WHERE id = 0 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't12' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't12' SYNC FORMAT Null;
SELECT id, k, n.a, n.c, n.m FROM t12 ORDER BY id;

SELECT 'patch carrying a recomputed MATERIALIZED member';
CREATE TABLE t13 (id UInt64, k UInt64, n Nested(a UInt64, c String), `n.m` Array(UInt64) MATERIALIZED range(k))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t13 (id, k, `n.a`, `n.c`) VALUES (0, 2, [1, 2], ['p', 'q']), (1, 1, [3], ['r']);
UPDATE t13 SET k = 1, `n.a` = [4] WHERE id = 1;
ALTER TABLE t13 APPLY PATCHES SETTINGS mutations_sync = 2;
SELECT id, k, n.a, n.c, n.m FROM t13 ORDER BY id;
UPDATE t13 SET k = 0 WHERE id = 0;
ALTER TABLE t13 APPLY PATCHES SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't13' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't13' SYNC FORMAT Null;
SELECT id, n.a, n.c FROM t13 ORDER BY id;

SELECT 'convert a stored column to an array of other sizes';
CREATE TABLE t14 (id UInt64, n Nested(a UInt64, c String), `n.x` String, `n.y` String) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t14 VALUES (0, [1, 2], ['p', 'q'], '[5, 6]', '[]'), (1, [3], ['r'], '[7]', '[]');
ALTER TABLE t14 MODIFY COLUMN `n.x` Array(UInt64) SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.x FROM t14 ORDER BY id;
ALTER TABLE t14 MODIFY COLUMN `n.y` Array(UInt64) SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't14' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't14' SYNC FORMAT Null;
SELECT id, n.a, n.c, n.x FROM t14 ORDER BY id;

SELECT 'rewrite after the conversion is killed';
ALTER TABLE t14 DELETE WHERE id = 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't14' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't14' SYNC FORMAT Null;
ALTER TABLE t14 UPDATE `n.y` = [] WHERE 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't14' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't14' SYNC FORMAT Null;
ALTER TABLE t14 UPDATE `n.y` = arrayMap(v -> v * 10, `n.a`) WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t14 DELETE WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT id, n.a, n.c, n.x, n.y FROM t14 ORDER BY id;

SELECT 'convert a stored column of a compact part to an array of other sizes';
CREATE TABLE t15 (id UInt64, n Nested(a UInt64, c String), `n.y` String) ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '10G';
INSERT INTO t15 VALUES (0, [1, 2], ['p', 'q'], '[]'), (1, [3], ['r'], '[]');
ALTER TABLE t15 MODIFY COLUMN `n.y` Array(UInt64) SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't15' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't15' SYNC FORMAT Null;
ALTER TABLE t15 DELETE WHERE id = 1 SETTINGS mutations_sync = 2; -- { serverError UNFINISHED }
SELECT position(latest_fail_reason, 'SIZES_OF_ARRAYS_DONT_MATCH') > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't15' AND NOT is_done;
KILL MUTATION WHERE database = currentDatabase() AND table = 't15' SYNC FORMAT Null;
SELECT id, n.a, n.c FROM t15 ORDER BY id;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;
DROP TABLE t9;
DROP TABLE t10;
DROP TABLE t11;
DROP TABLE t12;
DROP TABLE t13;
DROP TABLE t14;
DROP TABLE t15;
