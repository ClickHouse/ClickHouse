-- Each case warms the partial aggregate cache with one query and then runs a query that must not reuse the warmed
-- states. Results are correct with or without the cache, so other tests dropping the cache do not matter.

SET use_partial_aggregate_cache = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

SELECT '--- decimal scale of an aggregate function parameter';
DROP TABLE IF EXISTS t_pac_decimal;
CREATE TABLE t_pac_decimal (k UInt8, v Float64, ts UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pac_decimal VALUES (1, 100, 0), (1, 0, 1);
SELECT k, round(exponentialMovingAverage(toDecimal32(5, 0))(v, ts), 6) FROM t_pac_decimal GROUP BY k;
SELECT k, round(exponentialMovingAverage(toDecimal32(0.5, 1))(v, ts), 6) FROM t_pac_decimal GROUP BY k;
SELECT k, round(exponentialMovingAverage(toDecimal32(5, 0))(v, ts), 6) FROM t_pac_decimal GROUP BY k;
DROP TABLE t_pac_decimal;

SELECT '--- default expression reading a dictionary';
DROP DICTIONARY IF EXISTS d_pac_default;
DROP TABLE IF EXISTS t_pac_dict_source;
DROP TABLE IF EXISTS t_pac_dict_default;
CREATE TABLE t_pac_dict_source (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pac_dict_source VALUES (1, 10);
CREATE DICTIONARY d_pac_default (k UInt64, v UInt64)
PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 't_pac_dict_source')) LIFETIME(0) LAYOUT(FLAT());
CREATE TABLE t_pac_dict_default (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pac_dict_default VALUES (1), (1);
ALTER TABLE t_pac_dict_default ADD COLUMN v UInt64 DEFAULT dictGetUInt64('d_pac_default', 'v', k);
SELECT k, sum(v) FROM t_pac_dict_default GROUP BY k;
TRUNCATE TABLE t_pac_dict_source;
INSERT INTO t_pac_dict_source VALUES (1, 100);
SYSTEM RELOAD DICTIONARY d_pac_default;
SELECT k, sum(v) FROM t_pac_dict_default GROUP BY k;
DROP TABLE t_pac_dict_default;
DROP DICTIONARY d_pac_default;
DROP TABLE t_pac_dict_source;
