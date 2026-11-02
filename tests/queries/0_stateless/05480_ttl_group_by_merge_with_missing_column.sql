-- Merges of a table with `TTL ... GROUP BY` over a column that no source part stores.
-- https://github.com/ClickHouse/ClickHouse/issues/128051

SET optimize_throw_if_noop = 1;

CREATE TABLE t_due (k UInt64, id UInt64, d DateTime, s UInt64) ENGINE = MergeTree ORDER BY (k, id)
TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s)
SETTINGS enable_vertical_merge_algorithm = 0;
INSERT INTO t_due SELECT 1, number, '2000-01-01', 1 FROM numbers(10);
INSERT INTO t_due SELECT 1, number + 10, '2000-01-01', 1 FROM numbers(10);
ALTER TABLE t_due ADD COLUMN x UInt64;
OPTIMIZE TABLE t_due FINAL;
SELECT 'due', k, s, x FROM t_due;

CREATE TABLE t_not_due (k UInt64, id UInt64, d DateTime, s UInt64) ENGINE = MergeTree ORDER BY (k, id)
TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s)
SETTINGS enable_vertical_merge_algorithm = 0;
INSERT INTO t_not_due SELECT 1, number, '2100-01-01', 1 FROM numbers(10);
INSERT INTO t_not_due SELECT 1, number + 10, '2100-01-01', 1 FROM numbers(10);
ALTER TABLE t_not_due ADD COLUMN x UInt64;
OPTIMIZE TABLE t_not_due FINAL;
SELECT 'not due', count(), sum(s), sum(x) FROM t_not_due;

-- A column TTL removes `c` from the part, so the next merge reads it from no part.
CREATE TABLE t_column_ttl (k UInt64, id UInt64, d DateTime, s UInt64, c UInt64 TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY (k, id)
TTL d + INTERVAL 100 YEAR GROUP BY k SET s = sum(s)
SETTINGS enable_vertical_merge_algorithm = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_column_ttl SELECT 1, number, '2000-01-01', 1, 5 FROM numbers(10);
OPTIMIZE TABLE t_column_ttl FINAL;
SELECT 'parts storing c', count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_column_ttl' AND active AND column = 'c';
OPTIMIZE TABLE t_column_ttl FINAL;
SELECT 'column ttl', count(), sum(s), sum(c) FROM t_column_ttl;

CREATE TABLE t_set (k UInt64, id UInt64, d DateTime, s UInt64) ENGINE = MergeTree ORDER BY (k, id)
TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s)
SETTINGS enable_vertical_merge_algorithm = 0;
INSERT INTO t_set SELECT 1, number, '2000-01-01', 1 FROM numbers(10);
INSERT INTO t_set SELECT 1, number + 10, '2000-01-01', 1 FROM numbers(10);
ALTER TABLE t_set ADD COLUMN x UInt64;
ALTER TABLE t_set MODIFY TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s), x = sum(s) SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_set FINAL;
SELECT 'set', k, s, x FROM t_set;

-- A merge that does not apply the TTL leaves out a `SET` target that no source part stores.
CREATE TABLE t_set_not_due (k UInt64, id UInt64, d DateTime, s UInt64) ENGINE = MergeTree ORDER BY (k, id)
TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s)
SETTINGS enable_vertical_merge_algorithm = 0;
INSERT INTO t_set_not_due SELECT 1, number, '2100-01-01', 1 FROM numbers(10);
INSERT INTO t_set_not_due SELECT 1, number + 10, '2100-01-01', 1 FROM numbers(10);
ALTER TABLE t_set_not_due ADD COLUMN x UInt64;
ALTER TABLE t_set_not_due MODIFY TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s), x = sum(s) SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_set_not_due FINAL;
SELECT 'parts storing x', count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_set_not_due' AND active AND column = 'x';

-- With TTL merges stopped, a merge must neither aggregate the due rows nor lose the `SET` target.
CREATE TABLE t_set_stopped (k UInt64, id UInt64, d DateTime, s UInt64) ENGINE = MergeTree ORDER BY (k, id)
TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s)
SETTINGS enable_vertical_merge_algorithm = 0;
SYSTEM STOP TTL MERGES t_set_stopped;
INSERT INTO t_set_stopped SELECT 1, number, '2000-01-01', 1 FROM numbers(10);
INSERT INTO t_set_stopped SELECT 1, number + 10, '2000-01-01', 1 FROM numbers(10);
ALTER TABLE t_set_stopped ADD COLUMN x UInt64;
ALTER TABLE t_set_stopped MODIFY TTL d + INTERVAL 1 DAY GROUP BY k SET s = sum(s), x = sum(s) SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE t_set_stopped FINAL;
SELECT 'set stopped', count(), sum(s), sum(x) FROM t_set_stopped;
