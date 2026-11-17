-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- Over a `Nullable` column, `sum`, `avg`, `sumCount`, `min`, `max` and `count(column)` are answered from per-part
-- `basic` statistics: the sum of the non-NULL values, their min/max, and the NULL count. A part of NULLs only
-- contributes nothing, and NULL is the result when there is no other part.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_syntax_fuse_functions = 0, optimize_arithmetic_operations_in_aggregate_functions = 1;
-- `optimize_functions_to_subcolumns` rewrites `count(column)` over a `Nullable` column to `sum(NOT column.null)`, which
-- is not answered from statistics.
SET optimize_functions_to_subcolumns = 0;
-- `use_statistics_for_min_max_aggregation` and `use_statistics_for_sum_avg_aggregation` keep their defaults.
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;
SET materialize_statistics_on_insert = 1, mutations_sync = 2;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_only_null;
DROP TABLE IF EXISTS t_minmax;

-- `sparse` is mostly NULL, so it is stored with sparse serialization.
CREATE TABLE t_null
(
    i16 Nullable(Int16), i64 Nullable(Int64), u8 Nullable(UInt8), u256 Nullable(UInt256),
    d64 Nullable(Decimal64(4)), dt Nullable(DateTime('UTC')), f Nullable(Float64), s Nullable(String),
    sparse Nullable(Int32), lc LowCardinality(Nullable(Int32))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic', ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_null;

-- a part with NULLs in every column, a part without NULLs, and a part of NULLs only
INSERT INTO t_null SELECT
    if(number % 3 = 0, NULL, number * 31 - 1000), if(number % 5 = 0, NULL, number * 1000000007 - 500000000000),
    if(number % 7 = 0, NULL, number % 256),
    if(number % 11 = 0, NULL, toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') - number),
    if(number % 13 = 0, NULL, toDecimal64(number, 4) / 7), if(number % 17 = 0, NULL, toDateTime('2020-01-01 00:00:00', 'UTC') + number),
    if(number % 19 = 0, NULL, number / 3), if(number % 23 = 0, NULL, toString(number)),
    if(number % 10 = 0, number, NULL), if(number % 29 = 0, NULL, number)
FROM numbers(1000);

INSERT INTO t_null SELECT number, number, number, number, number, toDateTime(number, 'UTC'), number, toString(number), number, number
FROM numbers(100);

INSERT INTO t_null SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM numbers(10);

SELECT 'answered from statistics, the same as without them';
SELECT sum(i16), sum(i64), sum(u8), sum(u256), sum(d64), sum(sparse), count(i16), count(dt), count(s), count(sparse), count() FROM t_null;
SELECT sum(i16), sum(i64), sum(u8), sum(u256), sum(d64), sum(sparse), count(i16), count(dt), count(s), count(sparse), count() FROM t_null SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT avg(i16), avg(i64), avg(u8), avg(u256), avg(d64), avg(sparse), sumCount(i16), sumCount(d64) FROM t_null;
SELECT avg(i16), avg(i64), avg(u8), avg(u256), avg(d64), avg(sparse), sumCount(i16), sumCount(d64) FROM t_null SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT min(i16), max(i16), min(i64), max(u256), min(d64), max(dt), min(f), max(f), min(sparse), max(sparse) FROM t_null;
SELECT min(i16), max(i16), min(i64), max(u256), min(d64), max(dt), min(f), max(f), min(sparse), max(sparse) FROM t_null SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(i16), avg(i64), sumCount(d64), count(dt), min(f), max(u256), sum(sparse), count() FROM t_null);

SELECT 'a part without statistics is read, the others are answered from statistics';
INSERT INTO t_null (i16, d64) SETTINGS materialize_statistics_on_insert = 0 VALUES (-5, 5), (NULL, NULL);
SELECT sum(i16), avg(d64), count(i16), min(i16) FROM t_null;
SELECT sum(i16), avg(d64), count(i16), min(i16) FROM t_null SETTINGS use_statistics_for_sum_avg_aggregation = 0, use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(i16), avg(d64), count(i16), min(i16) FROM t_null);

-- `ATTACH` drops `SYSTEM STOP MERGES`, so only the use of the statistics is checked here, not which parts are read
SELECT 'statistics loaded from disk';
DETACH TABLE t_null;
ATTACH TABLE t_null;
SELECT sum(u256), avg(d64), count(s), min(dt), max(sparse) FROM t_null;
SELECT sum(u256), avg(d64), count(s), min(dt), max(sparse) FROM t_null SETTINGS use_statistics_for_sum_avg_aggregation = 0, use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(u256), avg(d64), count(s), min(dt), max(sparse) FROM t_null);

SELECT 'the merged part has the sum and the NULL count of the merged parts';
OPTIMIZE TABLE t_null FINAL;
SELECT sum(i16), avg(i64), sumCount(d64), min(u8), max(dt), count(s), count(sparse) FROM t_null;
SELECT sum(i16), avg(i64), sumCount(d64), min(u8), max(dt), count(s), count(sparse) FROM t_null SETTINGS use_statistics_for_sum_avg_aggregation = 0, use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(i16), avg(i64), sumCount(d64), min(u8), max(dt), count(s), count(sparse) FROM t_null);

SELECT 'not applied';
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(f) FROM t_null);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(s) FROM t_null);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(lc) FROM t_null);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(lc) FROM t_null);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT count(lc) FROM t_null);

SELECT 'only NULLs';
CREATE TABLE t_only_null (n Nullable(Int32), d Nullable(Decimal32(2))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic';
INSERT INTO t_only_null SELECT NULL, NULL FROM numbers(10);
SELECT sum(n), avg(n), sumCount(d), min(n), max(d), count(n), count() FROM t_only_null;
SELECT sum(n), avg(n), sumCount(d), min(n), max(d), count(n), count() FROM t_only_null SETTINGS use_statistics_for_sum_avg_aggregation = 0, use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(n), avg(n), sumCount(d), min(n), max(d), count(n), count() FROM t_only_null);

SELECT 'minmax statistics have no NULL count';
CREATE TABLE t_minmax (n Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax';
INSERT INTO t_minmax SELECT if(number % 2 = 0, NULL, number) FROM numbers(10);
SELECT min(n), max(n), count(n) FROM t_minmax;
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(n), max(n) FROM t_minmax);

DROP TABLE t_null;
DROP TABLE t_only_null;
DROP TABLE t_minmax;
