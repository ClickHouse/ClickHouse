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

CREATE TABLE t_identical (id UInt64, f Float64)
ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS columns_identical_across_versions = 'f'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_identical (id UInt64, a UInt32, b String, v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY id
SETTINGS columns_identical_across_versions = 'a, `b`';

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
