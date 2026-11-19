-- A MATERIALIZED column reached through a sorting-key subcolumn (`ORDER BY m.a`, `m MATERIALIZED tuple(x)`)
-- cannot be recomputed in an existing part: its sort order is fixed when the part is written. A value-changing
-- ALTER MODIFY COLUMN of the source is therefore rejected, like MATERIALIZE COLUMN refusing a key column.

DROP TABLE IF EXISTS t_materialized_subcolumn_key;

CREATE TABLE t_materialized_subcolumn_key (x Int64, m Tuple(a Int64) MATERIALIZED tuple(x), payload Int64)
ENGINE = MergeTree ORDER BY m.a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_materialized_subcolumn_key (x, payload) SELECT toInt64(3000000000) + number, number FROM numbers(16);

-- Int64 -> Int32 wraps the values, changing the stored key subcolumn m.a: rejected.
ALTER TABLE t_materialized_subcolumn_key MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- The ALTER did not apply, so the data and its order are intact.
SELECT count() FROM t_materialized_subcolumn_key WHERE m.a >= toInt64(3000000000);

DROP TABLE t_materialized_subcolumn_key;
