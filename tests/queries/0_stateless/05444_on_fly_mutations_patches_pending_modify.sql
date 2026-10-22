-- Lightweight updates and on-fly mutations read with `apply_mutations_on_fly = 1` while an `ALTER MODIFY COLUMN` is pending.
-- Every case except the last also reads the table after all mutations are materialized: both lines must be equal.

SET enable_lightweight_update = 1;

-- A lightweight UPDATE older than the MODIFY of the column the first on-fly step reads.
DROP TABLE IF EXISTS t_read_v1;
CREATE TABLE t_read_v1 (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';
INSERT INTO t_read_v1 SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_read_v1;
ALTER TABLE t_read_v1 UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_read_v1 SET a = 'zz' WHERE id < 5;
ALTER TABLE t_read_v1 MODIFY COLUMN a LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_read_v1', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_read_v1 ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_read_v1;
ALTER TABLE t_read_v1 DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_read_v1', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_read_v1 ORDER BY id);
DROP TABLE t_read_v1;

DROP TABLE IF EXISTS t_read_v2;
CREATE TABLE t_read_v2 (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';
INSERT INTO t_read_v2 SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_read_v2;
ALTER TABLE t_read_v2 UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_read_v2 SET a = 'zz' WHERE id < 5;
ALTER TABLE t_read_v2 MODIFY COLUMN a LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_read_v2', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_read_v2 ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_read_v2;
ALTER TABLE t_read_v2 DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_read_v2', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_read_v2 ORDER BY id);
DROP TABLE t_read_v2;

-- The lightweight UPDATE is newer than an on-fly UPDATE that overwrites the column on all rows or on some rows.
DROP TABLE IF EXISTS t_overwrite_all;
CREATE TABLE t_overwrite_all (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_overwrite_all SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_overwrite_all;
ALTER TABLE t_overwrite_all UPDATE a = 'q' WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_overwrite_all SET a = 'zz' WHERE id < 5;
ALTER TABLE t_overwrite_all MODIFY COLUMN a LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_overwrite_all', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_overwrite_all ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_overwrite_all;
ALTER TABLE t_overwrite_all DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_overwrite_all', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_overwrite_all ORDER BY id);
DROP TABLE t_overwrite_all;

DROP TABLE IF EXISTS t_overwrite_rows;
CREATE TABLE t_overwrite_rows (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_overwrite_rows SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_overwrite_rows;
ALTER TABLE t_overwrite_rows UPDATE a = 'q' WHERE id >= 5 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_overwrite_rows SET a = 'zz' WHERE id < 5;
ALTER TABLE t_overwrite_rows MODIFY COLUMN a LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_overwrite_rows', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_overwrite_rows ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_overwrite_rows;
ALTER TABLE t_overwrite_rows DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_overwrite_rows', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_overwrite_rows ORDER BY id);
DROP TABLE t_overwrite_rows;

-- A later on-fly UPDATE reads the patched column.
DROP TABLE IF EXISTS t_between_steps;
CREATE TABLE t_between_steps (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_between_steps SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_between_steps;
ALTER TABLE t_between_steps UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_between_steps SET a = 'zz' WHERE id < 5;
ALTER TABLE t_between_steps UPDATE c = concat(a, '#') WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_between_steps MODIFY COLUMN a LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_between_steps', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_between_steps ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_between_steps;
ALTER TABLE t_between_steps DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_between_steps', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_between_steps ORDER BY id);
DROP TABLE t_between_steps;

-- An on-fly UPDATE queued after the MODIFY reads the modified column for the rows it does not update.
DROP TABLE IF EXISTS t_post_modify_read;
CREATE TABLE t_post_modify_read (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_post_modify_read SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_post_modify_read;
ALTER TABLE t_post_modify_read UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_post_modify_read SET c = 'p' WHERE id < 5;
ALTER TABLE t_post_modify_read MODIFY COLUMN c LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_post_modify_read UPDATE c = 'q' WHERE id >= 8 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_post_modify_read', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_modify_read ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_post_modify_read;
ALTER TABLE t_post_modify_read DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_post_modify_read', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_modify_read ORDER BY id);
DROP TABLE t_post_modify_read;

-- A lightweight UPDATE newer than the MODIFY, followed by an on-fly UPDATE of the same column.
DROP TABLE IF EXISTS t_post_alter_patch;
CREATE TABLE t_post_alter_patch (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_post_alter_patch SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_post_alter_patch;
ALTER TABLE t_post_alter_patch UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_post_alter_patch MODIFY COLUMN c LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_post_alter_patch SET c = 'p' WHERE id < 5;
ALTER TABLE t_post_alter_patch UPDATE c = 'q' WHERE id >= 8 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_post_alter_patch', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_alter_patch ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_post_alter_patch;
ALTER TABLE t_post_alter_patch DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_post_alter_patch', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_alter_patch ORDER BY id);
DROP TABLE t_post_alter_patch;

DROP TABLE IF EXISTS t_post_alter_patch_discarded;
CREATE TABLE t_post_alter_patch_discarded (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_post_alter_patch_discarded SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_post_alter_patch_discarded;
ALTER TABLE t_post_alter_patch_discarded UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_post_alter_patch_discarded MODIFY COLUMN c LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_post_alter_patch_discarded SET c = 'p' WHERE id < 5;
ALTER TABLE t_post_alter_patch_discarded UPDATE c = 'q' WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_post_alter_patch_discarded', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_alter_patch_discarded ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_post_alter_patch_discarded;
ALTER TABLE t_post_alter_patch_discarded DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_post_alter_patch_discarded', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_alter_patch_discarded ORDER BY id);
DROP TABLE t_post_alter_patch_discarded;

-- An on-fly UPDATE queued after a MODIFY that changes the column representation, without and with a lightweight UPDATE.
DROP TABLE IF EXISTS t_post_modify_cast;
CREATE TABLE t_post_modify_cast (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_post_modify_cast SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_post_modify_cast;
ALTER TABLE t_post_modify_cast UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_post_modify_cast MODIFY COLUMN c UInt64 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_post_modify_cast UPDATE c = 1000 WHERE id >= 8 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_post_modify_cast', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_modify_cast ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_post_modify_cast;
ALTER TABLE t_post_modify_cast DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_post_modify_cast', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_modify_cast ORDER BY id);
DROP TABLE t_post_modify_cast;

DROP TABLE IF EXISTS t_post_modify_cast_patch;
CREATE TABLE t_post_modify_cast_patch (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_post_modify_cast_patch SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_post_modify_cast_patch;
ALTER TABLE t_post_modify_cast_patch UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_post_modify_cast_patch SET c = '77' WHERE id < 5;
ALTER TABLE t_post_modify_cast_patch MODIFY COLUMN c UInt64 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_post_modify_cast_patch UPDATE c = 1000 WHERE id >= 8 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_post_modify_cast_patch', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_modify_cast_patch ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_post_modify_cast_patch;
ALTER TABLE t_post_modify_cast_patch DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_post_modify_cast_patch', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_post_modify_cast_patch ORDER BY id);
DROP TABLE t_post_modify_cast_patch;

-- One lightweight UPDATE of a column the first step reads and of a column a later on-fly UPDATE overwrites.
DROP TABLE IF EXISTS t_patch_two_columns;
CREATE TABLE t_patch_two_columns (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch_two_columns SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_patch_two_columns;
ALTER TABLE t_patch_two_columns UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_patch_two_columns SET a = 'zz', c = 'p' WHERE id < 5;
ALTER TABLE t_patch_two_columns UPDATE c = 'q' WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_patch_two_columns MODIFY COLUMN c LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_patch_two_columns', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_patch_two_columns ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_patch_two_columns;
ALTER TABLE t_patch_two_columns DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_patch_two_columns', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_patch_two_columns ORDER BY id);
DROP TABLE t_patch_two_columns;

-- A lightweight UPDATE discarded by a later on-fly UPDATE of all rows.
DROP TABLE IF EXISTS t_discarded_before_overwrite;
CREATE TABLE t_discarded_before_overwrite (id UInt64, a String, b UInt64, c String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_discarded_before_overwrite SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_discarded_before_overwrite;
ALTER TABLE t_discarded_before_overwrite UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_discarded_before_overwrite SET c = 'p' WHERE id < 5;
ALTER TABLE t_discarded_before_overwrite UPDATE c = 'q' WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_discarded_before_overwrite MODIFY COLUMN c LowCardinality(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_discarded_before_overwrite', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_discarded_before_overwrite ORDER BY id SETTINGS apply_mutations_on_fly = 1);
SYSTEM START MERGES t_discarded_before_overwrite;
ALTER TABLE t_discarded_before_overwrite DELETE WHERE id = 999 SETTINGS mutations_sync = 2;
SELECT 't_discarded_before_overwrite', 'materialized', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_discarded_before_overwrite ORDER BY id);
DROP TABLE t_discarded_before_overwrite;

-- The part stores `c` as LowCardinality(String) after a killed MODIFY, and one v1 patch updates `a` and `c`.
DROP TABLE IF EXISTS t_killed_modify;
CREATE TABLE t_killed_modify (id UInt64, a String, b UInt64, c LowCardinality(String)) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';
INSERT INTO t_killed_modify SELECT number, toString(number), 0, toString(number) FROM numbers(10);
SYSTEM STOP MERGES t_killed_modify;
ALTER TABLE t_killed_modify MODIFY COLUMN c String SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_killed_modify' SYNC FORMAT Null;
ALTER TABLE t_killed_modify UPDATE b = toUInt64OrZero(a) + 100 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
UPDATE t_killed_modify SET a = 'zz', c = 'p' WHERE id < 5;
ALTER TABLE t_killed_modify UPDATE c = 'q' WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_killed_modify MODIFY COLUMN c Nullable(String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 't_killed_modify', 'on fly', groupArray(a), groupArray(b), groupArray(c) FROM (SELECT a, b, c FROM t_killed_modify ORDER BY id SETTINGS apply_mutations_on_fly = 1);
DROP TABLE t_killed_modify;
