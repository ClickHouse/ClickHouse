-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- The `Parquet` format of data parts is experimental and requires `allow_experimental_parquet_parts`
-- to be enabled by `CREATE`, `ALTER`, or the settings of a projection. Existing tables keep working without it.

DROP TABLE IF EXISTS t_parquet_flag;
DROP TABLE IF EXISTS t_parquet_flag_other;

SET allow_experimental_parquet_parts = 0;

CREATE TABLE t_parquet_flag (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_rows_for_parquet_part = 1; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_parquet_flag (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_parquet_part = 1; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_parquet_flag (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_level_for_parquet_part = 1; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_parquet_flag (k UInt64, PROJECTION p (SELECT k ORDER BY k) WITH SETTINGS (min_rows_for_parquet_part = 1)) ENGINE = MergeTree ORDER BY k; -- { serverError SUPPORT_IS_DISABLED }

-- Zero values do not enable the format.
CREATE TABLE t_parquet_flag_other (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_rows_for_parquet_part = 0;
ALTER TABLE t_parquet_flag_other MODIFY SETTING min_rows_for_parquet_part = 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_parquet_flag_other ADD PROJECTION p (SELECT k ORDER BY k) WITH SETTINGS (min_rows_for_parquet_part = 1); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_parquet_parts = 1;
CREATE TABLE t_parquet_flag (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS min_rows_for_parquet_part = 1;
ALTER TABLE t_parquet_flag_other MODIFY SETTING min_rows_for_parquet_part = 1;

-- The tables keep working without the setting.
SET allow_experimental_parquet_parts = 0;
INSERT INTO t_parquet_flag SELECT number FROM numbers(10);
INSERT INTO t_parquet_flag_other SELECT number FROM numbers(10);
SELECT table, part_type, sum(rows) FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_parquet_flag%' AND active GROUP BY table, part_type ORDER BY table;
SELECT sum(k) FROM t_parquet_flag;

-- Turning the format off does not need the setting.
ALTER TABLE t_parquet_flag MODIFY SETTING min_rows_for_parquet_part = 0;
ALTER TABLE t_parquet_flag_other RESET SETTING min_rows_for_parquet_part;

DROP TABLE t_parquet_flag;
DROP TABLE t_parquet_flag_other;
