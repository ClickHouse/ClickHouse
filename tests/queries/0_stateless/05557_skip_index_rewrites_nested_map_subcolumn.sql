-- Tags: no-parallel-replicas
-- The key's source column can itself be a subcolumn: for `c Tuple(m Map(String, Int32))` the key on
-- `c.m['k']` reads `c.m`, and `FunctionToSubcolumnsPass` rewrites the filter to read `c.m.key_k`.
-- The column `c` owns `c.m`, so it must not be treated as another column shadowing `c.m.key_k`,
-- and the primary key, the `minmax` skip index and the partition key must be used.
-- Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;
SET optimize_multiif_to_if = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_nested_map_pk;
DROP TABLE IF EXISTS t_nested_map_idx;
DROP TABLE IF EXISTS t_nested_map_part;

CREATE TABLE t_nested_map_pk (id UInt32, c Tuple(m Map(String, Int32)))
ENGINE = MergeTree ORDER BY c.m['k']
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_nested_map_pk SELECT number, tuple(map('k', toInt32(number))) FROM numbers(100);

SELECT 'primary key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_nested_map_pk WHERE c.m['k'] > 97)
WHERE explain LIKE '%Granules:%/%';
SELECT id FROM t_nested_map_pk WHERE c.m['k'] > 97 ORDER BY id;

CREATE TABLE t_nested_map_idx (id UInt32, c Tuple(m Map(String, Int32)), INDEX idx multiIf(c.m['k'] > 0, c.m['k'], 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_nested_map_idx SELECT number, tuple(map('k', toInt32(number))) FROM numbers(100);

SELECT 'minmax skip index';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_nested_map_idx WHERE multiIf(c.m['k'] > 0, c.m['k'], 0) > 97)
WHERE explain LIKE '%Name: idx%' OR explain LIKE '%Granules:%/%';
SELECT id FROM t_nested_map_idx WHERE multiIf(c.m['k'] > 0, c.m['k'], 0) > 97 ORDER BY id;

CREATE TABLE t_nested_map_part (id UInt32, c Tuple(m Map(String, Int32)))
ENGINE = MergeTree PARTITION BY multiIf(c.m['k'] > 49, 1, 0) ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_nested_map_part SELECT number, tuple(map('k', toInt32(number))) FROM numbers(100);

SELECT 'partition key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_nested_map_part WHERE multiIf(c.m['k'] > 49, 1, 0) = 1)
WHERE explain LIKE '%Parts:%/%';
SELECT count() FROM t_nested_map_part WHERE multiIf(c.m['k'] > 49, 1, 0) = 1;

DROP TABLE t_nested_map_pk;
DROP TABLE t_nested_map_idx;
DROP TABLE t_nested_map_part;
