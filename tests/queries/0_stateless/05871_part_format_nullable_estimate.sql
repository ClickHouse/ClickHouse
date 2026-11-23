-- The part format is chosen by an estimate of the part size in which a NULL costs only its null-map byte.
-- An all-NULL `Nullable(String)` column of 1M rows is 10 MB in memory but 1 MB in the estimate, so with
-- 4 MB of keys the first part stays below the 6 MB wide threshold, while the same column with values exceeds it.
DROP TABLE IF EXISTS t_part_format_nullable_estimate;

CREATE TABLE t_part_format_nullable_estimate (k UInt32, s Nullable(String))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 6000000, min_rows_for_wide_part = 0;

INSERT INTO t_part_format_nullable_estimate SELECT number, NULL FROM numbers(1000000);
INSERT INTO t_part_format_nullable_estimate SELECT number, toString(number) FROM numbers(1000000);

SELECT name, part_type, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_part_format_nullable_estimate' AND active
ORDER BY name;

DROP TABLE t_part_format_nullable_estimate;
