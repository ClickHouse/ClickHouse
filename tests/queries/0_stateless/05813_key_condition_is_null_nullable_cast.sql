-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- no-replicated-database / no-parallel-replicas: EXPLAIN output differs.
-- `(range) OR x IS NULL`, where `x` is `CAST(toStartOfHour(ts) AS Nullable(DateTime))` of a non-Nullable sort key
-- column `ts` (for example a column of a UNION ALL view whose other branch returns NULL), must keep min-max and
-- primary key pruning. The outer cast only adds Nullable, so it is peeled, and `isNull` of the non-Nullable, total
-- rest is constantly false. Partial functions under the cast must still raise.

SET enable_analyzer = 1;
SET optimize_use_implicit_projections = 0;

DROP VIEW IF EXISTS v_kc_null;
DROP TABLE IF EXISTS t_kc_null;
DROP TABLE IF EXISTS t2_kc_null;
DROP TABLE IF EXISTS pk_null_kc_null;
DROP TABLE IF EXISTS d32n_kc_null;

CREATE TABLE t_kc_null (tenant Int64, ts DateTime64(6, 'UTC'), id Int64)
ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (tenant, ts, id)
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;
-- 200000 rows, one per 300 s from 2024-11-01: 23 monthly partitions .
INSERT INTO t_kc_null SELECT number % 10, toDateTime64('2024-11-01 00:00:00', 6, 'UTC') + toIntervalSecond(number * 300), number
FROM numbers(200000);
OPTIMIZE TABLE t_kc_null FINAL;

CREATE TABLE t2_kc_null (tenant Int64, ts Nullable(DateTime64(6, 'UTC')), id Int64)
ENGINE = MergeTree ORDER BY (tenant, id);
INSERT INTO t2_kc_null SELECT 3, NULL, number FROM numbers(100);

CREATE VIEW v_kc_null AS
SELECT tenant, ts, toStartOfHour(ts) AS hr FROM t_kc_null
UNION ALL
SELECT tenant, ts, CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) AS hr FROM t2_kc_null;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_kc_null' AND active;

-- Q1: range OR IS NULL on the base table .
SELECT 'Q1 minmax prunes to 1/23', countIf(explain ILIKE '%Parts: 1/23%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_kc_null WHERE tenant = 3
  AND ((CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) > toDateTime('2025-11-13 08:00:00', 'UTC')
        AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) < toDateTime('2025-11-14 07:59:59', 'UTC'))
       OR CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NULL));
-- The primary key uses ts too: fewer granules than the `tenant = 3`-only condition.
SELECT 'Q1 pk prunes below tenant-only',
  (SELECT min(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/'))) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_kc_null WHERE tenant = 3
     AND ((CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) > toDateTime('2025-11-13 08:00:00', 'UTC')
           AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) < toDateTime('2025-11-14 07:59:59', 'UTC'))
          OR CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NULL)) WHERE explain ILIKE '%Granules: %/%')
  <
  (SELECT min(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/'))) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_kc_null WHERE tenant = 3) WHERE explain ILIKE '%Granules: %/%');
SELECT 'Q1 count', count() FROM t_kc_null WHERE tenant = 3
  AND ((CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) > toDateTime('2025-11-13 08:00:00', 'UTC')
        AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) < toDateTime('2025-11-14 07:59:59', 'UTC'))
       OR CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NULL);
SELECT 'Q1 count, no index', count() FROM t_kc_null WHERE tenant = 3
  AND ((CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) > toDateTime('2025-11-13 08:00:00', 'UTC')
        AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) < toDateTime('2025-11-14 07:59:59', 'UTC'))
       OR CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NULL)
  SETTINGS use_primary_key = 0, use_skip_indexes = 0, use_index_for_in_with_subqueries = 0;

-- Same with `<=> NULL` (isNotDistinctFrom branch).
SELECT 'Q1 <=> NULL minmax prunes to 1/23', countIf(explain ILIKE '%Parts: 1/23%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_kc_null WHERE tenant = 3
  AND ((CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) > toDateTime('2025-11-13 08:00:00', 'UTC')
        AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) < toDateTime('2025-11-14 07:59:59', 'UTC'))
       OR CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NOT DISTINCT FROM NULL));

-- Same with `toNullable` as the outer wrapper.
SELECT 'Q1 toNullable minmax prunes to 1/23', countIf(explain ILIKE '%Parts: 1/23%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_kc_null WHERE tenant = 3
  AND ((toNullable(toStartOfHour(ts)) > toDateTime('2025-11-13 08:00:00', 'UTC')
        AND toNullable(toStartOfHour(ts)) < toDateTime('2025-11-14 07:59:59', 'UTC'))
       OR toNullable(toStartOfHour(ts)) IS NULL));

-- Q2 (control): the range alone prunes before and after the fix.
SELECT 'Q2 minmax prunes to 1/23', countIf(explain ILIKE '%Parts: 1/23%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_kc_null WHERE tenant = 3
  AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) > toDateTime('2025-11-13 08:00:00', 'UTC')
  AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) < toDateTime('2025-11-14 07:59:59', 'UTC'));

-- isNull / isNotNull of the cast alone: constant false / true, results unchanged.
SELECT 'isNull(cast) count', count() FROM t_kc_null WHERE CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NULL;
SELECT 'isNotNull(cast) count', count() FROM t_kc_null WHERE CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NOT NULL;

-- The same through the UNION ALL view .
SELECT 'v1 minmax prunes to 1/23', countIf(explain ILIKE '%Parts: 1/23%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM v_kc_null WHERE tenant = 3
  AND ((hr > toDateTime('2025-11-13 08:00:00', 'UTC') AND hr < toDateTime('2025-11-14 07:59:59', 'UTC')) OR hr IS NULL));
SELECT 'v1 count', count() FROM v_kc_null WHERE tenant = 3
  AND ((hr > toDateTime('2025-11-13 08:00:00', 'UTC') AND hr < toDateTime('2025-11-14 07:59:59', 'UTC')) OR hr IS NULL);
SELECT 'v1 count, no index', count() FROM v_kc_null WHERE tenant = 3
  AND ((hr > toDateTime('2025-11-13 08:00:00', 'UTC') AND hr < toDateTime('2025-11-14 07:59:59', 'UTC')) OR hr IS NULL)
  SETTINGS use_primary_key = 0, use_skip_indexes = 0;

-- Declined: the outer cast changes the value domain (DateTime -> Date), so it is not peeled.
SELECT 'outer Date cast declines (no 1/23)', countIf(explain ILIKE '%Parts: 1/23%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_kc_null WHERE tenant = 3
  AND ((CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) > toDateTime('2025-11-13 08:00:00', 'UTC')
        AND CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) < toDateTime('2025-11-14 07:59:59', 'UTC'))
       OR CAST(toStartOfHour(ts) AS Nullable(Date)) IS NULL));

-- Nullable-key control: an identity cast to the key's own Nullable type prunes like bare `isNull(k)`.
CREATE TABLE pk_null_kc_null (k Nullable(UInt32)) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, allow_nullable_key = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO pk_null_kc_null SELECT number FROM numbers(100000);
INSERT INTO pk_null_kc_null SELECT NULL FROM numbers(5000);
OPTIMIZE TABLE pk_null_kc_null FINAL;
SELECT 'nullable key: isNull(CAST(k, Nullable(UInt32))) granules = isNull(k) granules',
  (SELECT min(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/'))) FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_null_kc_null WHERE isNull(CAST(k, 'Nullable(UInt32)'))) WHERE explain ILIKE '%Granules: %/%')
  =
  (SELECT min(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/'))) FROM (EXPLAIN indexes = 1 SELECT count() FROM pk_null_kc_null WHERE isNull(k)) WHERE explain ILIKE '%Granules: %/%');
SELECT 'nullable key: cast count', count() FROM pk_null_kc_null WHERE isNull(CAST(k, 'Nullable(UInt32)'));

-- Declined: the rest of the chain returns Nullable (`toDateTime(Nullable(Date32))`), so no constant and no bare atom.
CREATE TABLE d32n_kc_null (d Nullable(Date32)) ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 1024, index_granularity_bytes = 0, allow_nullable_key = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO d32n_kc_null SELECT toDate32('2000-01-01') + number FROM numbers(3000);
INSERT INTO d32n_kc_null SELECT NULL FROM numbers(1000);
OPTIMIZE TABLE d32n_kc_null FINAL;
SELECT 'nullable rest declines: full scan',
  countIf(explain ILIKE '%Granules:%' AND extract(explain, 'Granules: (\\d+)/') = extract(explain, 'Granules: \\d+/(\\d+)')) > 0
  AND countIf(explain ILIKE '%Granules:%' AND extract(explain, 'Granules: (\\d+)/') != extract(explain, 'Granules: \\d+/(\\d+)')) = 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM d32n_kc_null WHERE isNull(CAST(toDateTime(d), 'Nullable(DateTime)')));
SELECT 'nullable rest: count', count() FROM d32n_kc_null WHERE isNull(CAST(toDateTime(d), 'Nullable(DateTime)'));


-- v3: NeverNull needs every inner element to be total. Partial inner functions behind the outer
-- Nullable cast must still RAISE (the row-level `isNull` evaluates them), on non-Nullable and Nullable keys.
DROP TABLE IF EXISTS d32_kc_null;
DROP TABLE IF EXISTS d32n_raise_kc_null;
DROP TABLE IF EXISTS i64_kc_null;
DROP TABLE IF EXISTS i64n_kc_null;
CREATE TABLE d32_kc_null (d Date32) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1024, add_minmax_index_for_numeric_columns = 0;
INSERT INTO d32_kc_null VALUES ('1900-01-01'), ('2000-01-01');
CREATE TABLE d32n_raise_kc_null (d Nullable(Date32)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1024, allow_nullable_key = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO d32n_raise_kc_null VALUES ('1900-01-01'), ('2000-01-01'), (NULL);
SELECT count() FROM d32_kc_null WHERE CAST(toDateTime(d) AS Nullable(DateTime)) IS NULL SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM d32_kc_null WHERE CAST(toDateTime(d) AS Nullable(DateTime)) IS NOT DISTINCT FROM NULL SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM d32_kc_null WHERE CAST(toDateTime(d) AS Nullable(DateTime)) IS NOT NULL SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM d32n_raise_kc_null WHERE CAST(toDateTime(d) AS Nullable(DateTime)) IS NULL SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

CREATE TABLE i64_kc_null (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024, add_minmax_index_for_numeric_columns = 0;
INSERT INTO i64_kc_null SELECT number FROM numbers(10);
CREATE TABLE i64n_kc_null (k Nullable(Int64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024, allow_nullable_key = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO i64n_kc_null SELECT if(number = 5, NULL, number) FROM numbers(10);
SELECT count() FROM i64_kc_null WHERE CAST(intDiv(k, 0) AS Nullable(Int64)) IS NULL; -- { serverError ILLEGAL_DIVISION }
SELECT count() FROM i64_kc_null WHERE CAST(intDiv(k, 0) AS Nullable(Int64)) IS NOT DISTINCT FROM NULL; -- { serverError ILLEGAL_DIVISION }
SELECT count() FROM i64n_kc_null WHERE CAST(intDiv(k, 0) AS Nullable(Int64)) IS NULL; -- { serverError ILLEGAL_DIVISION }
-- A nullability-dropping inner cast under an outer Nullable cast raises on the NULL row.
SELECT count() FROM i64n_kc_null WHERE CAST(CAST(k, 'Int64') AS Nullable(Int64)) IS NULL; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- ALWAYS_TRUE is exact: the implicit-projection / exact-count path must return the full row count.
SELECT 'isNotNull(cast) exact count, implicit projections', count() FROM t_kc_null
WHERE CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NOT NULL SETTINGS optimize_use_implicit_projections = 1;
SELECT 'isNull(cast) exact count, implicit projections', count() FROM t_kc_null
WHERE CAST(toStartOfHour(ts) AS Nullable(DateTime('UTC'))) IS NULL SETTINGS optimize_use_implicit_projections = 1;

DROP TABLE d32_kc_null;
DROP TABLE d32n_raise_kc_null;
DROP TABLE i64_kc_null;
DROP TABLE i64n_kc_null;

DROP VIEW v_kc_null;
DROP TABLE t_kc_null;
DROP TABLE t2_kc_null;
DROP TABLE pk_null_kc_null;
DROP TABLE d32n_kc_null;
