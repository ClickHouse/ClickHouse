-- A MATERIALIZED column computed by a type-dependent function (`toTypeName(x)`) is recalculated when
-- ALTER MODIFY COLUMN changes the source type. The analyzer folds `toTypeName(x)` to a constant whose
-- dependency on `x` is hidden in its source expression, so the dependency must still be detected.

DROP TABLE IF EXISTS t_type_dependent_materialized;

CREATE TABLE t_type_dependent_materialized (x Int64, m String MATERIALIZED toTypeName(x))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_type_dependent_materialized (x) VALUES (1);
SELECT m FROM t_type_dependent_materialized;

ALTER TABLE t_type_dependent_materialized MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;
SELECT m FROM t_type_dependent_materialized;

DROP TABLE t_type_dependent_materialized;
