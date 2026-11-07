-- Validation of the table settings `columns_identical_across_versions` and `columns_varying_across_versions`.

DROP TABLE IF EXISTS t_identical;

CREATE TABLE t_identical (id UInt64, a UInt32, b String)
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS columns_identical_across_versions = 'a', columns_varying_across_versions = 'b'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_identical (id UInt64, a UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS columns_identical_across_versions = 'a'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_identical (id UInt64, a UInt32, v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY id
SETTINGS columns_identical_across_versions = 'missing'; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

CREATE TABLE t_identical (id UInt64, a UInt32, v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY id
SETTINGS columns_identical_across_versions = 'a, v'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_identical (id UInt64, meta Tuple(ver UInt64))
ENGINE = ReplacingMergeTree(meta.ver) ORDER BY id
SETTINGS columns_identical_across_versions = 'meta'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_identical (id UInt64, f Float64)
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS columns_identical_across_versions = 'f'; -- { serverError BAD_ARGUMENTS }

-- The expression of a listed column must give the same value for all versions of a row.
CREATE TABLE t_identical (id UInt64, created DateTime DEFAULT now())
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS columns_identical_across_versions = 'created'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_identical (id UInt64, score UInt32, bucket UInt32 MATERIALIZED intDiv(score, 100))
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS columns_identical_across_versions = 'bucket'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_identical (id UInt64, a UInt32, b String, v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY id
SETTINGS columns_identical_across_versions = 'a, `b`';

ALTER TABLE t_identical MODIFY COLUMN a UInt32 DEFAULT rand(); -- { serverError BAD_ARGUMENTS }

-- A listed column cannot be dropped or renamed while it is listed.
ALTER TABLE t_identical DROP COLUMN a; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_identical RENAME COLUMN b TO c; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
ALTER TABLE t_identical MODIFY SETTING columns_varying_across_versions = 'v'; -- { serverError BAD_ARGUMENTS }

ALTER TABLE t_identical MODIFY SETTING columns_identical_across_versions = 'a';
ALTER TABLE t_identical RENAME COLUMN b TO c;

-- Switch to the opposite setting: all columns except the listed ones are identical.
ALTER TABLE t_identical RESET SETTING columns_identical_across_versions;
ALTER TABLE t_identical MODIFY SETTING columns_varying_across_versions = 'c';
ALTER TABLE t_identical DROP COLUMN a;
ALTER TABLE t_identical DROP COLUMN c; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_identical' ORDER BY position;

DROP TABLE t_identical;

-- An `UPDATE` of a listed column must update all versions of a row in the same way.
DROP TABLE IF EXISTS t_update;
CREATE TABLE t_update (id UInt64, a UInt32, b UInt32, c UInt32, v UInt64)
ENGINE = ReplacingMergeTree(v) PARTITION BY b ORDER BY id
SETTINGS columns_identical_across_versions = 'a', enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_update VALUES (1, 10, 0, 0, 1), (1, 10, 1, 0, 2);

ALTER TABLE t_update UPDATE a = 20 WHERE v = 2; -- { serverError CANNOT_UPDATE_COLUMN }
ALTER TABLE t_update UPDATE a = c WHERE id = 1; -- { serverError CANNOT_UPDATE_COLUMN }
ALTER TABLE t_update UPDATE a = 20 IN PARTITION 1 WHERE id = 1; -- { serverError CANNOT_UPDATE_COLUMN }
UPDATE t_update SET a = 20 WHERE v = 2; -- { serverError CANNOT_UPDATE_COLUMN }

ALTER TABLE t_update UPDATE a = a + 10, c = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
UPDATE t_update SET a = 40, c = 2 WHERE id = 1;
UPDATE t_update SET c = 3 WHERE v = 2;
SELECT a, c FROM t_update ORDER BY v;

DROP TABLE t_update;
