-- A lambda argument that shadows a table column must not be recorded as a dependency on that column.
-- `m MATERIALIZED arraySum(arrayMap(x -> x + 1, [1, 2]))` does not read the table column `x`, so an
-- ALTER MODIFY COLUMN x is allowed even though m is a key column (ORDER BY m).

DROP TABLE IF EXISTS t_lambda_shadow;

CREATE TABLE t_lambda_shadow (x Int64, m UInt64 MATERIALIZED arraySum(arrayMap(x -> x + 1, [1, 2])))
ENGINE = MergeTree ORDER BY m
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_lambda_shadow (x) VALUES (10);

ALTER TABLE t_lambda_shadow MODIFY COLUMN x Int32 SETTINGS mutations_sync = 2;

SELECT x, m FROM t_lambda_shadow;

DROP TABLE t_lambda_shadow;
