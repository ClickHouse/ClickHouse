DROP TABLE IF EXISTS t_lwu_dependent_of_added_consumer;
DROP TABLE IF EXISTS t_lwu_dependent_of_added_consumer_mutation;

-- `z` reads the patched `a` and is evaluated again, `y` is evaluated again because it reads the missing `b`
-- through which `z` reads too, and `w` is evaluated again because it reads `y`: all three agree in a row.

CREATE TABLE t_lwu_dependent_of_added_consumer (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lwu_dependent_of_added_consumer VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_dependent_of_added_consumer;

ALTER TABLE t_lwu_dependent_of_added_consumer ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_dependent_of_added_consumer ADD COLUMN b UInt64 DEFAULT rand64();
ALTER TABLE t_lwu_dependent_of_added_consumer ADD COLUMN z UInt64 DEFAULT a + b;
ALTER TABLE t_lwu_dependent_of_added_consumer ADD COLUMN y UInt64 DEFAULT b;
ALTER TABLE t_lwu_dependent_of_added_consumer ADD COLUMN w UInt64 DEFAULT y;

UPDATE t_lwu_dependent_of_added_consumer SET a = 5 WHERE 1;
SELECT id, a, z = y + a, y = w FROM t_lwu_dependent_of_added_consumer ORDER BY id;

UPDATE t_lwu_dependent_of_added_consumer SET a = 7 WHERE id = 0;
SELECT id, a, z = y + a, y = w FROM t_lwu_dependent_of_added_consumer ORDER BY id;

-- The same when the patch is applied at the boundary of the step applying a pending mutation on the fly.

CREATE TABLE t_lwu_dependent_of_added_consumer_mutation (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lwu_dependent_of_added_consumer_mutation VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_dependent_of_added_consumer_mutation;

ALTER TABLE t_lwu_dependent_of_added_consumer_mutation ADD COLUMN a UInt64 DEFAULT 5, ADD COLUMN v UInt32 DEFAULT 7;
ALTER TABLE t_lwu_dependent_of_added_consumer_mutation ADD COLUMN b UInt64 DEFAULT rand64();
ALTER TABLE t_lwu_dependent_of_added_consumer_mutation ADD COLUMN z UInt64 DEFAULT a + b;
ALTER TABLE t_lwu_dependent_of_added_consumer_mutation ADD COLUMN y UInt64 DEFAULT b;
ALTER TABLE t_lwu_dependent_of_added_consumer_mutation ADD COLUMN w UInt64 DEFAULT y;

ALTER TABLE t_lwu_dependent_of_added_consumer_mutation UPDATE v = 8 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_dependent_of_added_consumer_mutation SET a = 7 WHERE id = 0;

SELECT 'pending mutation';
SELECT id, a, v, z = y + a, y = w FROM t_lwu_dependent_of_added_consumer_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lwu_dependent_of_added_consumer;
DROP TABLE t_lwu_dependent_of_added_consumer_mutation;
