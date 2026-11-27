-- The primary key and `minmax` index analysis must not prune granules when a `Decimal` column is compared
-- with a float. The comparison is done in `Float64`, where many `Decimal` values equal the same float, and
-- converting the float to the `Decimal` type can be inexact: `toFloat32(1)` becomes `0.99999998430674944`.
-- https://github.com/ClickHouse/ClickHouse/issues/126494

SET use_query_condition_cache = 0, use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_decimal_float_key;
DROP TABLE IF EXISTS t_decimal_float_minmax;
DROP TABLE IF EXISTS t_decimal_float_expression_key;
DROP TABLE IF EXISTS t_decimal_float_expression_minmax;
DROP TABLE IF EXISTS t_datetime64_float_key;
DROP TABLE IF EXISTS t_decimal_exact_float_key;
DROP TABLE IF EXISTS t_decimal_exact_float_hash_key;
DROP TABLE IF EXISTS t_decimal_exact_float_minmax;
DROP TABLE IF EXISTS t_decimal_float32_set_key;
DROP TABLE IF EXISTS t_decimal_float32_set_minmax;
DROP TABLE IF EXISTS t_decimal_float32_set_partition;
DROP TABLE IF EXISTS t_decimal_wide_value_key;
DROP TABLE IF EXISTS t_decimal_wide_value_minmax;
DROP TABLE IF EXISTS t_decimal_wide_value_partition;

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

-- A function of the column as the key: its monotonic chain would cast the constant to the `Decimal` argument too.
SELECT 'key expression';

CREATE TABLE t_decimal_float_expression_key (d Decimal(38, 18), ts UInt32)
ENGINE = MergeTree ORDER BY toFloat64(d) SETTINGS index_granularity = 1;

INSERT INTO t_decimal_float_expression_key SELECT d, ts FROM t_decimal_float_key;

SELECT arraySort(groupArray(ts)) FROM t_decimal_float_expression_key WHERE d = toFloat32(1);
-- Casting the constant also threw: "Conversion from BFloat16 to Decimal is not implemented".
SELECT count() FROM t_decimal_float_expression_key WHERE d = toBFloat16(1);

CREATE TABLE t_decimal_float_expression_minmax (d Decimal(38, 18), ts UInt32, INDEX i toFloat64(d) TYPE minmax)
ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;

INSERT INTO t_decimal_float_expression_minmax SELECT d, ts FROM t_decimal_float_key;

SELECT count() FROM t_decimal_float_expression_minmax WHERE d = toFloat32(1);

-- `DateTime64` is compared with a float as `Float64` as well, where values nanoseconds apart are equal.
SELECT 'DateTime64';

CREATE TABLE t_datetime64_float_key (d DateTime64(9, 'UTC'), ts UInt32)
ENGINE = MergeTree ORDER BY (d, ts) SETTINGS index_granularity = 1;

INSERT INTO t_datetime64_float_key VALUES ('2026-01-01 00:00:00', 10), ('2026-01-01 00:00:00.000000001', 20), ('2026-01-01 00:00:00.000000002', 30), ('2026-01-01 00:00:02', 40);

SELECT arraySort(groupArray(ts)) FROM t_datetime64_float_key WHERE d = toFloat64(1767225600);
SELECT arraySort(groupArray(ts)) FROM t_datetime64_float_key WHERE d <= toFloat64(1767225600);

-- The float elements of a subquery set are cast to the key type in another place, with the same problem.
SELECT 'IN';

SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d IN (SELECT toFloat32(1));
SELECT arraySort(groupArray(ts)) FROM t_decimal_float_key WHERE d IN (SELECT toFloat64(1));
SELECT count() FROM t_decimal_float_key WHERE d NOT IN (SELECT toFloat32(1));
SELECT count() FROM t_decimal_float_key WHERE d IN (SELECT toBFloat16(1));
SELECT count() FROM t_decimal_float_minmax WHERE d IN (SELECT toFloat32(1));

-- A float that converts to the `Decimal` and back unchanged, while the values one step below and above the cast value
-- convert to other floats, equals exactly that value, so the key analysis still uses it, as for `d = 20.00`: the literal
-- is a `Float64`.
SELECT 'exact float';

CREATE TABLE t_decimal_exact_float_key (d Decimal(10, 2), ts UInt32)
ENGINE = MergeTree ORDER BY (d, ts) SETTINGS index_granularity = 1;

INSERT INTO t_decimal_exact_float_key VALUES ('0.28', 10), ('0.29', 20), ('0.30', 30), ('20', 40), ('20.01', 50);

SELECT arraySort(groupArray(ts)) FROM t_decimal_exact_float_key WHERE d = 20.00 SETTINGS force_primary_key = 1;
SELECT arraySort(groupArray(ts)) FROM t_decimal_exact_float_key WHERE d IN (SELECT 20.00) SETTINGS force_primary_key = 1;
-- `0.29` is `0.28999999999999998` as a float, which the cast truncates to `0.28`.
SELECT arraySort(groupArray(ts)) FROM t_decimal_exact_float_key WHERE d = 0.29;

-- A deterministic function of the column as the key, like the bucket partitions of Iceberg: the constant is cast to the
-- column type before the function is applied.
CREATE TABLE t_decimal_exact_float_hash_key (d Decimal(10, 2), ts UInt32)
ENGINE = MergeTree ORDER BY cityHash64(d) SETTINGS index_granularity = 1;

INSERT INTO t_decimal_exact_float_hash_key SELECT d, ts FROM t_decimal_exact_float_key;

SELECT arraySort(groupArray(ts)) FROM t_decimal_exact_float_hash_key WHERE d = 20.00 SETTINGS force_primary_key = 1;
SELECT arraySort(groupArray(ts)) FROM t_decimal_exact_float_hash_key WHERE d = 0.29;

-- A set compares the bits of a float: `0` converts to `0.0`, which is not the element `-0.0`, so every row is
-- `NOT IN (SELECT -0.0)`, while `= -0.0` holds for `0`.
CREATE TABLE t_decimal_exact_float_minmax (d Decimal(10, 2), ts UInt32, INDEX i d TYPE minmax)
ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;

INSERT INTO t_decimal_exact_float_minmax SELECT d, ts FROM t_decimal_exact_float_key;
INSERT INTO t_decimal_exact_float_minmax VALUES ('0', 60);

SELECT count() FROM t_decimal_exact_float_minmax WHERE d NOT IN (SELECT -0.0);
SELECT count() FROM t_decimal_exact_float_minmax WHERE d IN (SELECT -0.0);
SELECT count() FROM t_decimal_exact_float_minmax WHERE d = -0.0;

-- `IN` compares the value with the elements in their own type: as a `Float32`, every value from `999999.97` to `1000000.03`
-- is `toFloat32(1000000)`, while as a `Float64` only `1000000.00` is `toFloat64(1000000)`.
SELECT 'Float32 set';

CREATE TABLE t_decimal_float32_set_key (d Decimal(9, 2), ts UInt32)
ENGINE = MergeTree ORDER BY (d, ts) SETTINGS index_granularity = 1;

INSERT INTO t_decimal_float32_set_key VALUES ('999999.97', 1), ('999999.98', 2), ('999999.99', 3), ('1000000.00', 4), ('1000000.01', 5), ('1000000.02', 6), ('1000000.03', 7), ('2000000.00', 8), ('20', 9);

SELECT arraySort(groupArray(ts)) FROM t_decimal_float32_set_key WHERE d IN (SELECT toFloat32(1000000));
SELECT arraySort(groupArray(ts)) FROM t_decimal_float32_set_key WHERE d IN (SELECT toFloat64(1000000)) SETTINGS force_primary_key = 1;
SELECT arraySort(groupArray(ts)) FROM t_decimal_float32_set_key WHERE d IN (SELECT toFloat32(20)) SETTINGS force_primary_key = 1;

CREATE TABLE t_decimal_float32_set_minmax (d Decimal(9, 2), ts UInt32, INDEX i d TYPE minmax)
ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;

INSERT INTO t_decimal_float32_set_minmax SELECT d, ts FROM t_decimal_float32_set_key;

SELECT arraySort(groupArray(ts)) FROM t_decimal_float32_set_minmax WHERE d IN (SELECT toFloat32(1000000));

CREATE TABLE t_decimal_float32_set_partition (d Decimal(9, 2), ts UInt32)
ENGINE = MergeTree PARTITION BY cityHash64(d) % 8 ORDER BY ts SETTINGS index_granularity = 1;

INSERT INTO t_decimal_float32_set_partition SELECT d, ts FROM t_decimal_float32_set_key;

SELECT arraySort(groupArray(ts)) FROM t_decimal_float32_set_partition WHERE d IN (SELECT toFloat32(1000000));

-- The storage type bounds the stored values, not the precision: `INSERT SELECT` stores 16 digits in `Decimal(15, 0)`, where
-- `9007199254740995` and `9007199254740997` are the same `Float64` as `9007199254740996`.
SELECT 'beyond precision';

CREATE TABLE t_decimal_wide_value_key (d Decimal(15, 0), ts UInt32)
ENGINE = MergeTree ORDER BY (d, ts) SETTINGS index_granularity = 1;

INSERT INTO t_decimal_wide_value_key SELECT toDecimal64(9007199254740992 + number, 0), number FROM numbers(10);

SELECT arraySort(groupArray(ts)) FROM t_decimal_wide_value_key WHERE d = toFloat64(9007199254740996);
SELECT arraySort(groupArray(ts)) FROM t_decimal_wide_value_key WHERE d IN (SELECT toFloat64(9007199254740996));

CREATE TABLE t_decimal_wide_value_minmax (d Decimal(15, 0), ts UInt32, INDEX i d TYPE minmax)
ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;

INSERT INTO t_decimal_wide_value_minmax SELECT d, ts FROM t_decimal_wide_value_key;

SELECT arraySort(groupArray(ts)) FROM t_decimal_wide_value_minmax WHERE d = toFloat64(9007199254740996);

CREATE TABLE t_decimal_wide_value_partition (d Decimal(15, 0), ts UInt32)
ENGINE = MergeTree PARTITION BY cityHash64(d) % 8 ORDER BY ts SETTINGS index_granularity = 1;

INSERT INTO t_decimal_wide_value_partition SELECT d, ts FROM t_decimal_wide_value_key;

SELECT arraySort(groupArray(ts)) FROM t_decimal_wide_value_partition WHERE d = toFloat64(9007199254740996);

DROP TABLE t_decimal_float_key;
DROP TABLE t_decimal_float_minmax;
DROP TABLE t_decimal_float_expression_key;
DROP TABLE t_decimal_float_expression_minmax;
DROP TABLE t_datetime64_float_key;
DROP TABLE t_decimal_exact_float_key;
DROP TABLE t_decimal_exact_float_hash_key;
DROP TABLE t_decimal_exact_float_minmax;
DROP TABLE t_decimal_float32_set_key;
DROP TABLE t_decimal_float32_set_minmax;
DROP TABLE t_decimal_float32_set_partition;
DROP TABLE t_decimal_wide_value_key;
DROP TABLE t_decimal_wide_value_minmax;
DROP TABLE t_decimal_wide_value_partition;
