-- A key on `c.a['k']` must not be matched by `c.a.key_k` when that name refers to another element of
-- the `Tuple` `c`: for ``c Tuple(`a.key_k` Int32, a Map(String, Int32))``, `c.a.key_k` is the element
-- `a.key_k`, not the key `k` of the `Map` `c.a`, and `FunctionToSubcolumnsPass` does not rewrite to it.
-- Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_sibling_pk;
DROP TABLE IF EXISTS t_sibling_idx;
DROP TABLE IF EXISTS t_sibling_part;
DROP TABLE IF EXISTS t_no_sibling;

CREATE TABLE t_sibling_pk (c Tuple(`a.key_k` Int32, a Map(String, Int32)))
ENGINE = MergeTree ORDER BY c.a['k']
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_sibling_pk SELECT tuple(toInt32(number), map('k', toInt32(100 + number))) FROM numbers(100);

SELECT count() FROM t_sibling_pk WHERE c.a.key_k = 99;
SELECT count() FROM t_sibling_pk WHERE c.a.key_k = 99 SETTINGS use_primary_key = 0;

CREATE TABLE t_sibling_idx (c Tuple(`a.key_k` Int32, a Map(String, Int32)), INDEX idx c.a['k'] TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_sibling_idx SELECT tuple(toInt32(number), map('k', toInt32(100 + number))) FROM numbers(100);

SELECT count() FROM t_sibling_idx WHERE c.a.key_k = 99;
SELECT count() FROM t_sibling_idx WHERE c.a.key_k = 99 SETTINGS use_skip_indexes = 0;

CREATE TABLE t_sibling_part (c Tuple(`a.key_k` Int32, a Map(String, Int32)))
ENGINE = MergeTree PARTITION BY c.a['k'] ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_sibling_part SELECT tuple(toInt32(number), map('k', toInt32(100 + number))) FROM numbers(10);

SELECT count() FROM t_sibling_part WHERE c.a.key_k = 9;
SELECT count() FROM t_sibling_part WHERE c.a.key_k = 9 SETTINGS use_partition_pruning = 0;

-- Partition pruning of a mutation.
ALTER TABLE t_sibling_part DELETE WHERE c.a.key_k = 9 SETTINGS mutations_sync = 2;
SELECT count() FROM t_sibling_part WHERE c.a.key_k = 9 SETTINGS use_partition_pruning = 0;

-- Without the sibling element, the rewritten filter still uses the key.
CREATE TABLE t_no_sibling (c Tuple(b Int32, a Map(String, Int32)))
ENGINE = MergeTree ORDER BY c.a['k']
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_no_sibling SELECT tuple(toInt32(number), map('k', toInt32(100 + number))) FROM numbers(100);
SELECT count() FROM t_no_sibling WHERE c.a['k'] = 150 SETTINGS force_primary_key = 1;

DROP TABLE t_sibling_pk;
DROP TABLE t_sibling_idx;
DROP TABLE t_sibling_part;
DROP TABLE t_no_sibling;
