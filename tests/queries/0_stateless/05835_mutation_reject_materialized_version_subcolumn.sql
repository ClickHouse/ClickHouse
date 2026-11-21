-- A MATERIALIZED column used as the engine version column through a subcolumn (`ReplacingMergeTree(m.v)`,
-- `m MATERIALIZED tuple(x)`) cannot be recomputed in an existing part, so a value-changing ALTER of its
-- source is rejected: the guard resolves the version subcolumn `m.v` to its storage column `m`.

DROP TABLE IF EXISTS t_materialized_version_subcolumn;

CREATE TABLE t_materialized_version_subcolumn (id UInt64, x UInt64, m Tuple(v UInt64) MATERIALIZED tuple(x))
ENGINE = ReplacingMergeTree(m.v) ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_materialized_version_subcolumn (id, x) VALUES (1, 4294967297);

ALTER TABLE t_materialized_version_subcolumn MODIFY COLUMN x UInt32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT x, m.v FROM t_materialized_version_subcolumn;

DROP TABLE t_materialized_version_subcolumn;
