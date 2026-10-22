-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: a replicated engine applies the metadata of MODIFY COLUMN asynchronously, so the UPDATE may still write the old type.

-- A patch part of a lightweight UPDATE stores a column in another type than the data part, and an
-- ALTER MODIFY COLUMN of that column is pending. A killed MODIFY COLUMN leaves the data part's type
-- behind the metadata, so the patch is written in the metadata type of that time.

SET enable_lightweight_update = 1;

-- Part LowCardinality(Nullable(String)), patch Nullable(String), pending MODIFY to Nullable(UInt64).
-- The mutation that follows applies the patch while executing the pending MODIFY.
DROP TABLE IF EXISTS t_lwu_type_a;
CREATE TABLE t_lwu_type_a (id UInt64, a LowCardinality(Nullable(String)), b UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu_type_a SELECT number, toString(number), 0 FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_a;
ALTER TABLE t_lwu_type_a MODIFY COLUMN a Nullable(String) SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_a' SYNC FORMAT Null;
UPDATE t_lwu_type_a SET a = '42' WHERE id < 5;
ALTER TABLE t_lwu_type_a MODIFY COLUMN a Nullable(UInt64) SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'A types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_a' AND active AND column = 'a' ORDER BY startsWith(name, 'patch-'));
SELECT 'A read', groupArray(a) FROM (SELECT a FROM t_lwu_type_a ORDER BY id);
SYSTEM START MERGES t_lwu_type_a;
ALTER TABLE t_lwu_type_a UPDATE b = 1 WHERE id = 0 SETTINGS mutations_sync = 2;
SELECT 'A mutated', groupArray(a), groupArray(b) FROM (SELECT a, b FROM t_lwu_type_a ORDER BY id);
SELECT 'A mutations not done', countIf(NOT is_done) FROM system.mutations WHERE database = currentDatabase() AND table = 't_lwu_type_a';
DROP TABLE t_lwu_type_a;

-- Part Nullable(UInt64), patch Nullable(String) for `a` and UInt8 for `b` (the type of `b` in the part),
-- pending MODIFY of `a` to LowCardinality(Nullable(String)).
-- v1 patch parts: one patch updates a and b, which are applied at different stages.
DROP TABLE IF EXISTS t_lwu_type_b;
CREATE TABLE t_lwu_type_b (id UInt64, a Nullable(UInt64), b UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';
INSERT INTO t_lwu_type_b SELECT number, number, 0 FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_b;
ALTER TABLE t_lwu_type_b MODIFY COLUMN a Nullable(String) SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_b' SYNC FORMAT Null;
UPDATE t_lwu_type_b SET a = 'abc', b = 1 WHERE id < 5;
ALTER TABLE t_lwu_type_b MODIFY COLUMN a LowCardinality(Nullable(String)) SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'B types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_b' AND active AND column = 'a' ORDER BY startsWith(name, 'patch-'));
SELECT 'B read', groupArray(a), groupArray(b) FROM (SELECT a, b FROM t_lwu_type_b ORDER BY id);
DROP TABLE t_lwu_type_b;

-- Part UInt8, patch UInt64 with a value that does not fit into UInt8, pending MODIFY to UInt32.
DROP TABLE IF EXISTS t_lwu_type_c;
CREATE TABLE t_lwu_type_c (id UInt64, a UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu_type_c SELECT number, number FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_c;
ALTER TABLE t_lwu_type_c MODIFY COLUMN a UInt64 SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_c' SYNC FORMAT Null;
UPDATE t_lwu_type_c SET a = 1000 WHERE id < 5;
ALTER TABLE t_lwu_type_c MODIFY COLUMN a UInt32 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'C types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_c' AND active AND column = 'a' ORDER BY startsWith(name, 'patch-'));
SELECT 'C read', groupArray(a) FROM (SELECT a FROM t_lwu_type_c ORDER BY id);
DROP TABLE t_lwu_type_c;

-- A pending ALTER UPDATE applied on the fly overwrites `a` before anything reads it, and the patch value
-- cannot be cast into the type of `a` in the data part.
DROP TABLE IF EXISTS t_lwu_type_d;
CREATE TABLE t_lwu_type_d (id UInt64, a UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu_type_d SELECT number, number FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_d;
ALTER TABLE t_lwu_type_d MODIFY COLUMN a Nullable(String) SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_d' SYNC FORMAT Null;
UPDATE t_lwu_type_d SET a = 'abc' WHERE id < 5;
ALTER TABLE t_lwu_type_d UPDATE a = '7' WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_lwu_type_d MODIFY COLUMN a Nullable(UInt64) SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'D types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_d' AND active AND column = 'a' ORDER BY startsWith(name, 'patch-'));
SELECT 'D read on fly', groupArray(a) FROM (SELECT a FROM t_lwu_type_d ORDER BY id SETTINGS apply_mutations_on_fly = 1);
DROP TABLE t_lwu_type_d;

-- Part Int64, patch Int32, pending MODIFY to Date32. Int32 and Date32 are stored alike,
-- and the cast clamps the patch value to the lower bound of Date32.
DROP TABLE IF EXISTS t_lwu_type_e;
CREATE TABLE t_lwu_type_e (id UInt64, a Int64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu_type_e SELECT number, number FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_e;
ALTER TABLE t_lwu_type_e MODIFY COLUMN a Int32 SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_e' SYNC FORMAT Null;
UPDATE t_lwu_type_e SET a = -1000000 WHERE id < 5;
ALTER TABLE t_lwu_type_e MODIFY COLUMN a Date32 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'E types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_e' AND active AND column = 'a' ORDER BY startsWith(name, 'patch-'));
SELECT 'E read', groupArray(toInt32(a)) FROM (SELECT a FROM t_lwu_type_e ORDER BY id);
DROP TABLE t_lwu_type_e;

-- The same with v1 patch parts, two such patches that also update a String column, which are applied together.
DROP TABLE IF EXISTS t_lwu_type_f;
CREATE TABLE t_lwu_type_f (id UInt64, a Int64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';
INSERT INTO t_lwu_type_f SELECT number, number, '' FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_f;
ALTER TABLE t_lwu_type_f MODIFY COLUMN a Int32 SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_f' SYNC FORMAT Null;
UPDATE t_lwu_type_f SET a = -1000000, s = 'x' WHERE id < 5;
UPDATE t_lwu_type_f SET a = 100, s = 'y' WHERE id < 2;
ALTER TABLE t_lwu_type_f MODIFY COLUMN a Date32 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'F types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_f' AND active AND column = 'a' ORDER BY startsWith(name, 'patch-'));
SELECT 'F read', groupArray(toInt32(a)), groupArray(s) FROM (SELECT a, s FROM t_lwu_type_f ORDER BY id);
DROP TABLE t_lwu_type_f;

-- Parts Int32 and Nullable(Int32), patches Date32 and Nullable(Int64), and a pending MODIFY of `b` to Int64
-- with a DEFAULT that reads `a`: the NULLs of `b` are replaced from `a` with its patch applied.
DROP TABLE IF EXISTS t_lwu_type_g;
CREATE TABLE t_lwu_type_g (id UInt64, a Int32, b Nullable(Int32)) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu_type_g SELECT number, number, NULL FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_g;
ALTER TABLE t_lwu_type_g MODIFY COLUMN a Date32, MODIFY COLUMN b Nullable(Int64) SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_g' SYNC FORMAT Null;
UPDATE t_lwu_type_g SET a = toDate32(1000) WHERE id < 5;
UPDATE t_lwu_type_g SET b = 7 WHERE id = 4;
ALTER TABLE t_lwu_type_g MODIFY COLUMN b Int64 DEFAULT intDiv(1000000, toInt64(a)) SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'G types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_g' AND active AND column IN ('a', 'b') ORDER BY startsWith(name, 'patch-'), column);
SELECT 'G read', groupArray(toInt32(a)), groupArray(b) FROM (SELECT a, b FROM t_lwu_type_g ORDER BY id);
DROP TABLE t_lwu_type_g;

-- Parts Nullable(Int32) and Nullable(UInt8), patches Nullable(Int64) with a NULL and Nullable(String), and killed MODIFY
-- of `a` to Int64 with a DEFAULT and of `d` to Dynamic: the NULL of the patch of `a` is replaced by the DEFAULT, `b` reads
-- the patched `a`, and the patch of `d` keeps its values.
DROP TABLE IF EXISTS t_lwu_type_h;
CREATE TABLE t_lwu_type_h (id UInt64, a Nullable(Int32), b Nullable(Int32), d Nullable(UInt8)) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_lwu_type_h SELECT number, number, NULL, number FROM numbers(10);
SYSTEM STOP MERGES t_lwu_type_h;
ALTER TABLE t_lwu_type_h MODIFY COLUMN a Nullable(Int64), MODIFY COLUMN d Nullable(String) SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_h' SYNC FORMAT Null;
UPDATE t_lwu_type_h SET a = if(id = 0, NULL, 1000), d = 'abc' WHERE id < 5;
ALTER TABLE t_lwu_type_h MODIFY COLUMN a Int64 DEFAULT 42, MODIFY COLUMN d Dynamic DEFAULT 42 SETTINGS alter_sync = 0, mutations_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_lwu_type_h' SYNC FORMAT Null;
ALTER TABLE t_lwu_type_h MODIFY COLUMN b Int64 DEFAULT a SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT 'H types', groupArray(type) FROM (SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_lwu_type_h' AND active AND column IN ('a', 'd') ORDER BY startsWith(name, 'patch-'), column);
SELECT 'H read', groupArray(a), groupArray(b), groupArray(toString(d)) FROM (SELECT a, b, d FROM t_lwu_type_h ORDER BY id);
DROP TABLE t_lwu_type_h;
