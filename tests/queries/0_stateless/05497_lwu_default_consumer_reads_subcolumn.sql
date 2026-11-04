DROP TABLE IF EXISTS t_lwu_consumer_reads_subcolumn;
DROP TABLE IF EXISTS t_lwu_consumer_reads_subcolumn_mutation;

-- `z` reads the patched `a` and the missing `b`, so it is evaluated again with a new value of `b`.
-- `y` reads `b` only through its subcolumn `b.x`, and it has to be evaluated again from the same value of `b`.

SELECT 'v1';
CREATE TABLE t_lwu_consumer_reads_subcolumn (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_consumer_reads_subcolumn VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_consumer_reads_subcolumn;

ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN b Tuple(x UInt64, y UInt64) DEFAULT tuple(rand64(), rand64());
ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN z UInt64 DEFAULT a + tupleElement(b, 1);
ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN y UInt64 DEFAULT b.x;

SELECT id, a, z = y + a FROM t_lwu_consumer_reads_subcolumn ORDER BY id;

UPDATE t_lwu_consumer_reads_subcolumn SET a = 5 WHERE 1;
SELECT id, a, z = y + a FROM t_lwu_consumer_reads_subcolumn ORDER BY id;

UPDATE t_lwu_consumer_reads_subcolumn SET a = 7 WHERE id = 0;
SELECT id, a, z = y + a FROM t_lwu_consumer_reads_subcolumn ORDER BY id;

DROP TABLE t_lwu_consumer_reads_subcolumn;

SELECT 'v2';
CREATE TABLE t_lwu_consumer_reads_subcolumn (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_consumer_reads_subcolumn VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_consumer_reads_subcolumn;

ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN b Tuple(x UInt64, y UInt64) DEFAULT tuple(rand64(), rand64());
ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN z UInt64 DEFAULT a + tupleElement(b, 1);
ALTER TABLE t_lwu_consumer_reads_subcolumn ADD COLUMN y UInt64 DEFAULT b.x;

SELECT id, a, z = y + a FROM t_lwu_consumer_reads_subcolumn ORDER BY id;

UPDATE t_lwu_consumer_reads_subcolumn SET a = 5 WHERE 1;
SELECT id, a, z = y + a FROM t_lwu_consumer_reads_subcolumn ORDER BY id;

UPDATE t_lwu_consumer_reads_subcolumn SET a = 7 WHERE id = 0;
SELECT id, a, z = y + a FROM t_lwu_consumer_reads_subcolumn ORDER BY id;

-- The same when the patch is applied at the boundary of the step applying a pending mutation on the fly.

CREATE TABLE t_lwu_consumer_reads_subcolumn_mutation (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lwu_consumer_reads_subcolumn_mutation VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_consumer_reads_subcolumn_mutation;

ALTER TABLE t_lwu_consumer_reads_subcolumn_mutation ADD COLUMN a UInt64 DEFAULT 5, ADD COLUMN v UInt32 DEFAULT 7;
ALTER TABLE t_lwu_consumer_reads_subcolumn_mutation ADD COLUMN b Tuple(x UInt64, y UInt64) DEFAULT tuple(rand64(), rand64());
ALTER TABLE t_lwu_consumer_reads_subcolumn_mutation ADD COLUMN z UInt64 DEFAULT a + tupleElement(b, 1);
ALTER TABLE t_lwu_consumer_reads_subcolumn_mutation ADD COLUMN y UInt64 DEFAULT b.x;

ALTER TABLE t_lwu_consumer_reads_subcolumn_mutation UPDATE v = 8 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_consumer_reads_subcolumn_mutation SET a = 5 WHERE 1;

SELECT 'pending mutation';
SELECT id, a, v, z = y + a FROM t_lwu_consumer_reads_subcolumn_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lwu_consumer_reads_subcolumn;
DROP TABLE t_lwu_consumer_reads_subcolumn_mutation;
