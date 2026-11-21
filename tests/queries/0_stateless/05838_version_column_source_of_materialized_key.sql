-- The engine's version column may change its type in some ways, but when a MATERIALIZED key column is computed
-- from it, the key column cannot be recalculated in an existing part, so the type change is rejected.

DROP TABLE IF EXISTS t_version_source_of_key;
DROP TABLE IF EXISTS t_version_plain;

CREATE TABLE t_version_source_of_key (v UInt32, m String MATERIALIZED toTypeName(v), s String)
ENGINE = ReplacingMergeTree(v) ORDER BY m
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_version_source_of_key (v, s) VALUES (1, 'payload');

ALTER TABLE t_version_source_of_key MODIFY COLUMN v UInt64 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT m = toTypeName(v) FROM t_version_source_of_key;

-- Without such a column the version column can still be widened.
CREATE TABLE t_version_plain (v UInt32, s String) ENGINE = ReplacingMergeTree(v) ORDER BY s;
INSERT INTO t_version_plain VALUES (1, 'payload');
ALTER TABLE t_version_plain MODIFY COLUMN v UInt64 SETTINGS mutations_sync = 2;
SELECT toTypeName(v) FROM t_version_plain;

DROP TABLE t_version_source_of_key;
DROP TABLE t_version_plain;
