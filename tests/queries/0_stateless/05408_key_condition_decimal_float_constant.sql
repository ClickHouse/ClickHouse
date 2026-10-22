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

-- In a `Decimal` of at most 15 digits, every value is a different `Float64`. A float that converts to the type and back
-- unchanged then equals exactly one value, so the key analysis still uses it, as for `d = 20.00`: the literal is a `Float64`.
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

DROP TABLE t_decimal_float_key;
DROP TABLE t_decimal_float_minmax;
DROP TABLE t_decimal_float_expression_key;
DROP TABLE t_decimal_float_expression_minmax;
DROP TABLE t_datetime64_float_key;
DROP TABLE t_decimal_exact_float_key;
DROP TABLE t_decimal_exact_float_hash_key;
