-- A MATERIALIZED column used as the engine's version column cannot be recomputed in an existing part
-- (it fixes merge semantics like a key), so a value-changing ALTER MODIFY COLUMN of its source is
-- rejected, even though the version column is not part of the sorting/partition key.

DROP TABLE IF EXISTS t_materialized_version;

CREATE TABLE t_materialized_version (d Int64, x Int64, v UInt64 MATERIALIZED toUInt64(x))
ENGINE = ReplacingMergeTree(v) ORDER BY d
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_materialized_version (d, x) SELECT number, number FROM numbers(4);

-- Int64 -> Int32 preserves these values but is a value-changing conversion in general: rejected.
ALTER TABLE t_materialized_version MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT count() FROM t_materialized_version;

DROP TABLE t_materialized_version;
