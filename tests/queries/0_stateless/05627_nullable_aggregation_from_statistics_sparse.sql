-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.
-- no-random-merge-tree-settings: the columns must be stored with sparse serialization.

-- `basic` statistics of a `Nullable` column with sparse serialization, whose NULLs are its defaults: `sum`, `avg`,
-- `sumCount`, `min`, `max` and `count(column)` answered from them are the same as without them. `z` also has non-NULL
-- zeros, which are values, not defaults.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_syntax_fuse_functions = 0, optimize_arithmetic_operations_in_aggregate_functions = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;
SET materialize_statistics_on_insert = 1, mutations_sync = 2;

DROP TABLE IF EXISTS t_sparse_null;

CREATE TABLE t_sparse_null (a Nullable(Int32), z Nullable(Int64), d Nullable(Decimal64(2)), e Nullable(Int32))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic', nullable_serialization_version = 'allow_sparse',
    ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_sparse_null;

-- a part with values, and a part of NULLs only
INSERT INTO t_sparse_null SELECT
    if(number % 10 = 0, number::Int32 - 500, NULL), if(number % 5 = 0, if(number % 10 = 0, 0, number * 1000003), NULL),
    if(number % 10 = 0, toDecimal64(number, 2) / 3, NULL), NULL
FROM numbers(1000);
INSERT INTO t_sparse_null SELECT NULL, NULL, NULL, NULL FROM numbers(10);

SELECT name, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_null' AND active ORDER BY name, column;

SELECT 'answered from statistics, the same as without them';
SELECT sum(a), sum(z), sum(d), sum(e), avg(a), avg(z), sumCount(d), count(a), count(z), count(e), count() FROM t_sparse_null;
SELECT sum(a), sum(z), sum(d), sum(e), avg(a), avg(z), sumCount(d), count(a), count(z), count(e), count() FROM t_sparse_null
SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT min(a), max(a), min(z), max(z), min(d), max(d), min(e), max(e) FROM t_sparse_null;
SELECT min(a), max(a), min(z), max(z), min(d), max(d), min(e), max(e) FROM t_sparse_null SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(a), avg(z), sumCount(d), count(e), min(a), max(z) FROM t_sparse_null);

SELECT 'the merged part has the sum and the NULL count of the merged parts';
SYSTEM START MERGES t_sparse_null;
OPTIMIZE TABLE t_sparse_null FINAL;
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_null' AND active ORDER BY column;
SELECT sum(a), sum(z), sum(d), avg(a), count(z), count(e), min(z), max(d) FROM t_sparse_null;
SELECT sum(a), sum(z), sum(d), avg(a), count(z), count(e), min(z), max(d) FROM t_sparse_null
SETTINGS use_statistics_for_sum_avg_aggregation = 0, use_statistics_for_min_max_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(a), sum(z), sum(d), avg(a), count(z), count(e), min(z), max(d) FROM t_sparse_null);

DROP TABLE t_sparse_null;
