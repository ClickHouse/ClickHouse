-- The primary key and `minmax` index analysis must not prune granules when a `Decimal` column is compared
-- with a float. The comparison is done in `Float64`, where many `Decimal` values equal the same float, and
-- converting the float to the `Decimal` type can be inexact: `toFloat32(1)` becomes `0.99999998430674944`.
-- https://github.com/ClickHouse/ClickHouse/issues/126494

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_decimal_float_key;
DROP TABLE IF EXISTS t_decimal_float_minmax;

CREATE TABLE t_decimal_float_key (d Decimal(38, 18), ts UInt32)
ENGINE = MergeTree ORDER BY (d, ts) SETTINGS index_granularity = 1;

INSERT INTO t_decimal_float_key VALUES ('1', 20), ('1', 30), ('1.000000000000000001', 10), ('1.000000000000000001', 20), ('2', 5);

SELECT 'primary key';
SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d = toFloat32(1);
SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d = toFloat64(1);
SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d = CAST(toFloat32(1) AS Dynamic);
SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d = CAST(toFloat64(1) AS Dynamic);
SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d <=> toFloat32(1);
SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d = toFloat32(1) SETTINGS use_primary_key = 0;

-- Constants that are not floats still use the primary key.
SELECT count() FROM t_decimal_float_key WHERE d = 1 SETTINGS force_primary_key = 1;
SELECT count() FROM t_decimal_float_key WHERE d = toDecimal32(2, 1) SETTINGS force_primary_key = 1;

SELECT 'minmax';

CREATE TABLE t_decimal_float_minmax (d Decimal(38, 18), ts UInt32, INDEX i d TYPE minmax)
ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;

INSERT INTO t_decimal_float_minmax SELECT d, ts FROM t_decimal_float_key;

SELECT count() FROM t_decimal_float_minmax WHERE d = toFloat32(1);
SELECT count() FROM t_decimal_float_minmax WHERE d = toFloat64(1);
SELECT count() FROM t_decimal_float_minmax WHERE d = CAST(toFloat32(1) AS Dynamic);
SELECT count() FROM t_decimal_float_minmax WHERE d = toFloat32(1) SETTINGS use_skip_indexes = 0;

DROP TABLE t_decimal_float_key;
DROP TABLE t_decimal_float_minmax;
