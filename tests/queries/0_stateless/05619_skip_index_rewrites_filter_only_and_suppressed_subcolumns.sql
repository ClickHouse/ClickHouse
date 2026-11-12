-- Tags: no-parallel-replicas
-- A key expression can use one `Map` that `FunctionToSubcolumnsPass` rewrites only in a filter
-- (`m['k']` to `m.key_k`, while `toString(m)` keeps the whole column) and another `Map` whose rewrite
-- the query declines (`n['k']` stays because of `mapKeys(n)` in the projection). The filter must be
-- matched by the key with both decisions applied together. Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;
SET optimize_multiif_to_if = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS t_part;

CREATE TABLE t_pk (id UInt32, m Map(String, Int32), n Map(String, Int32))
ENGINE = MergeTree ORDER BY m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_pk SELECT number, map('k', toInt32(number)), map('k', toInt32(number)) FROM numbers(100);

SELECT 'primary key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1
    SELECT mapKeys(n) FROM t_pk WHERE m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0) > 195)
WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_pk WHERE m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0) > 195 AND notEmpty(mapKeys(n));
SELECT count() FROM t_pk WHERE m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0) > 195 AND notEmpty(mapKeys(n))
SETTINGS use_primary_key = 0;

CREATE TABLE t_idx (id UInt32, m Map(String, Int32), n Map(String, Int32),
    INDEX idx m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_idx SELECT number, map('k', toInt32(number)), map('k', toInt32(number)) FROM numbers(100);

SELECT 'skip index';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1
    SELECT mapKeys(n) FROM t_idx WHERE m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0) > 195)
WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_idx WHERE m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0) > 195 AND notEmpty(mapKeys(n));
SELECT count() FROM t_idx WHERE m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0) > 195 AND notEmpty(mapKeys(n))
SETTINGS use_skip_indexes = 0;

CREATE TABLE t_part (id UInt32, m Map(String, Int32), n Map(String, Int32))
ENGINE = MergeTree PARTITION BY intDiv(m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0), 50) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_part SELECT number, map('k', toInt32(number)), map('k', toInt32(number)) FROM numbers(100);

SELECT 'partition key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1
    SELECT mapKeys(n) FROM t_part WHERE intDiv(m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0), 50) = 4)
WHERE explain LIKE '%Parts:%/%';
SELECT count() FROM t_part WHERE intDiv(m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0), 50) = 4 AND notEmpty(mapKeys(n));
SELECT count() FROM t_part WHERE intDiv(m['k'] + length(toString(m)) + multiIf(n['k'] > 0, n['k'], 0), 50) = 4 AND notEmpty(mapKeys(n))
SETTINGS use_partition_pruning = 0;

DROP TABLE t_pk;
DROP TABLE t_idx;
DROP TABLE t_part;
