-- A part written before `ADD COLUMN ... TTL` does not store the column: merging it with a part whose
-- values of the column have all expired still applies the column TTL.

DROP TABLE IF EXISTS t_column_ttl_add_column;

CREATE TABLE t_column_ttl_add_column (d DateTime, x UInt64)
ENGINE = MergeTree ORDER BY x PARTITION BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_number_of_merges_with_ttl_in_pool = 0;

SYSTEM STOP MERGES t_column_ttl_add_column;
INSERT INTO t_column_ttl_add_column SELECT '2100-01-01 00:00:00', number FROM numbers(100);
ALTER TABLE t_column_ttl_add_column ADD COLUMN s String DEFAULT 'dflt' TTL d + INTERVAL 1 DAY;
INSERT INTO t_column_ttl_add_column SELECT '2000-01-01 00:00:00', number + 1000, 'old' FROM numbers(100);

SELECT 'parts, parts storing s', uniqExact(part_name), countIf(column = 's') FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_column_ttl_add_column' AND active;
SELECT 'before merge', s, count() FROM t_column_ttl_add_column GROUP BY s ORDER BY s;

SYSTEM START MERGES t_column_ttl_add_column;
OPTIMIZE TABLE t_column_ttl_add_column FINAL;
SELECT 'after merge', s, count() FROM t_column_ttl_add_column GROUP BY s ORDER BY s;

DROP TABLE t_column_ttl_add_column;
