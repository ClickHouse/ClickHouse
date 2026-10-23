-- Tags: no-parallel-replicas
-- The rewrite-aware matching of skip indexes, primary keys and partition keys (see
-- `05023_skip_index_analyzer_rewrites`) must use the analyzer-rewritten form of a key expression only
-- when it computes the same values as the stored key.
SET explain_query_plan_default = 'legacy';
SET least_greatest_legacy_null_behavior = 0;
-- The injected split of the read ranges into intersecting and non-intersecting ones filters the rows by
-- the sorting key computed with the query's settings, which differs from the stored key in the cases below
-- regardless of the index analysis checked here.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- 1. The query's settings fold a constant subexpression of the key to a different value than it has in
-- the stored key: `greatest(NULL, 1)` is 1 for the stored key and NULL with the legacy behavior, so the
-- rewritten form of the key would be `a` instead of the stored `b`.

DROP TABLE IF EXISTS test_rewrites_settings_pk;
CREATE TABLE test_rewrites_settings_pk (a Int32, b Int32)
ENGINE = MergeTree ORDER BY if(isNull(greatest(NULL, 1)), a, b)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_rewrites_settings_pk SELECT number, 100 + number FROM numbers(100);
SELECT 'settings_primary_key', count() FROM test_rewrites_settings_pk WHERE a = 99 SETTINGS least_greatest_legacy_null_behavior = 1;
SELECT 'settings_primary_key', count() FROM test_rewrites_settings_pk WHERE a = 99 SETTINGS least_greatest_legacy_null_behavior = 1, use_primary_key = 0;
DROP TABLE test_rewrites_settings_pk;

DROP TABLE IF EXISTS test_rewrites_settings_partition;
CREATE TABLE test_rewrites_settings_partition (a Int32, b Int32)
ENGINE = MergeTree PARTITION BY if(isNull(greatest(NULL, 1)), a, b) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_rewrites_settings_partition SELECT number, 100 + number FROM numbers(100);
SELECT 'settings_partition_key', count() FROM test_rewrites_settings_partition WHERE a = 99 SETTINGS least_greatest_legacy_null_behavior = 1;
SELECT 'settings_partition_key', count() FROM test_rewrites_settings_partition WHERE a = 99 SETTINGS least_greatest_legacy_null_behavior = 1, use_partition_pruning = 0;
DROP TABLE test_rewrites_settings_partition;

-- The folded value can also stay inside the rewritten expression instead of dropping a column.
DROP TABLE IF EXISTS test_rewrites_settings_plus;
CREATE TABLE test_rewrites_settings_plus (a Int32)
ENGINE = MergeTree ORDER BY a + toInt32(isNull(greatest(NULL, 1)))
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_rewrites_settings_plus SELECT number FROM numbers(100);
SELECT 'settings_folded_constant', count() FROM test_rewrites_settings_plus WHERE a + toInt32(isNull(greatest(NULL, 1))) = 99 SETTINGS least_greatest_legacy_null_behavior = 1;
SELECT 'settings_folded_constant', count() FROM test_rewrites_settings_plus WHERE a + toInt32(isNull(greatest(NULL, 1))) = 99 SETTINGS least_greatest_legacy_null_behavior = 1, use_primary_key = 0;
DROP TABLE test_rewrites_settings_plus;

-- 2. The rewrites collapse the key expression to one of its inputs: `if(1 = 1, a, b)` is `a`.
-- An input of the original key expression named like the rewritten form must not be taken for the key column.

DROP TABLE IF EXISTS test_rewrites_collapse;
CREATE TABLE test_rewrites_collapse (a Int32, b Int32, INDEX idx if(1 = 1, a, b) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_rewrites_collapse SELECT number, 100 + number FROM numbers(100);
SELECT 'collapse', count() FROM test_rewrites_collapse WHERE a = 99;
SELECT 'collapse', count() FROM test_rewrites_collapse WHERE a = 99 SETTINGS use_skip_indexes = 0;
SELECT 'collapse', count() FROM test_rewrites_collapse WHERE a IN (98, 99);
DROP TABLE test_rewrites_collapse;

-- 3. `FunctionToSubcolumnsPass` does not rewrite `isNull(x)` to `x.null` when `x` is a key or an index
-- column, while it still rewrites `multiIf` to `if`: the rewritten form of the key must be computed the same way.

DROP TABLE IF EXISTS test_rewrites_indexed_skip;
CREATE TABLE test_rewrites_indexed_skip (x Nullable(Int32), v Int32, INDEX idx if(isNull(x), multiIf(v > 0, v, 0), 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO test_rewrites_indexed_skip SELECT NULL, number FROM numbers(100);
SELECT 'indexed_skip_index', countIf(explain LIKE '%Condition: (if(isNull(x), multiIf(greater(v, 0), v, 0), 0) in [51, 51])%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_rewrites_indexed_skip WHERE if(isNull(x), multiIf(v > 0, v, 0), 0) = 51);
SELECT 'indexed_skip_index', count() FROM test_rewrites_indexed_skip WHERE if(isNull(x), multiIf(v > 0, v, 0), 0) = 51;
SELECT 'indexed_skip_index', count() FROM test_rewrites_indexed_skip WHERE if(isNull(x), multiIf(v > 0, v, 0), 0) = 51 SETTINGS use_skip_indexes = 0;
DROP TABLE test_rewrites_indexed_skip;

DROP TABLE IF EXISTS test_rewrites_indexed_pk;
CREATE TABLE test_rewrites_indexed_pk (x Nullable(Int32), v Int32)
ENGINE = MergeTree ORDER BY if(isNull(x), multiIf(v > 0, v, 0), 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0, allow_nullable_key = 1;
INSERT INTO test_rewrites_indexed_pk SELECT NULL, number FROM numbers(100);
SELECT 'indexed_primary_key', countIf(explain LIKE '%Condition: (if(isNull(x), multiIf(greater(v, 0), v, 0), 0) in [51, 51])%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_rewrites_indexed_pk WHERE if(isNull(x), multiIf(v > 0, v, 0), 0) = 51);
SELECT 'indexed_primary_key', count() FROM test_rewrites_indexed_pk WHERE if(isNull(x), multiIf(v > 0, v, 0), 0) = 51;
SELECT 'indexed_primary_key', count() FROM test_rewrites_indexed_pk WHERE if(isNull(x), multiIf(v > 0, v, 0), 0) = 51 SETTINGS use_primary_key = 0;
DROP TABLE test_rewrites_indexed_pk;

DROP TABLE IF EXISTS test_rewrites_indexed_partition;
CREATE TABLE test_rewrites_indexed_partition (x Nullable(Int32), v Int32)
ENGINE = MergeTree PARTITION BY if(isNull(x), multiIf(v > 50, 1, 0), 0) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0, allow_nullable_key = 1;
INSERT INTO test_rewrites_indexed_partition SELECT NULL, number FROM numbers(100);
SELECT
    'indexed_partition_key',
    countIf(explain LIKE '%Condition: (if(isNull(x), multiIf(greater(v, 50), 1, 0), 0) in [1, 1])%'),
    countIf(explain LIKE '%Parts: 1/2%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_rewrites_indexed_partition WHERE if(isNull(x), multiIf(v > 50, 1, 0), 0) = 1 SETTINGS optimize_use_implicit_projections = 0);
SELECT 'indexed_partition_key', count() FROM test_rewrites_indexed_partition WHERE if(isNull(x), multiIf(v > 50, 1, 0), 0) = 1;
SELECT 'indexed_partition_key', count() FROM test_rewrites_indexed_partition WHERE if(isNull(x), multiIf(v > 50, 1, 0), 0) = 1 SETTINGS use_partition_pruning = 0;
DROP TABLE test_rewrites_indexed_partition;
