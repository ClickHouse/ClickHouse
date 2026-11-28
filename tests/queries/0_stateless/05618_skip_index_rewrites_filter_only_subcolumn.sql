-- Tags: no-parallel-replicas
-- `FunctionToSubcolumnsPass` rewrites `m['k']` to the subcolumn `m.key_k` in a filter even if the
-- whole `m` is used as well, e.g. in `m['k'] + length(toString(m))`, but not in a projection. A key on
-- such an expression must be matched by the filter. Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS t_part;

CREATE TABLE t_pk (id UInt32, m Map(String, Int32))
ENGINE = MergeTree ORDER BY m['k'] + length(toString(m))
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_pk SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'primary key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_pk WHERE m['k'] + length(toString(m)) > 100)
WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_pk WHERE m['k'] + length(toString(m)) > 100;
SELECT count() FROM t_pk WHERE m['k'] + length(toString(m)) > 100 SETTINGS use_primary_key = 0;

CREATE TABLE t_idx (id UInt32, m Map(String, Int32), INDEX idx m['k'] + length(toString(m)) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_idx SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'skip index';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_idx WHERE m['k'] + length(toString(m)) > 100)
WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_idx WHERE m['k'] + length(toString(m)) > 100;
SELECT count() FROM t_idx WHERE m['k'] + length(toString(m)) > 100 SETTINGS use_skip_indexes = 0;

CREATE TABLE t_part (id UInt32, m Map(String, Int32))
ENGINE = MergeTree PARTITION BY intDiv(m['k'] + length(toString(m)), 50) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_part SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'partition key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_part WHERE intDiv(m['k'] + length(toString(m)), 50) = 2)
WHERE explain LIKE '%Parts:%/%';
SELECT count() FROM t_part WHERE intDiv(m['k'] + length(toString(m)), 50) = 2;
SELECT count() FROM t_part WHERE intDiv(m['k'] + length(toString(m)), 50) = 2 SETTINGS use_partition_pruning = 0;

DROP TABLE t_pk;
DROP TABLE t_idx;
DROP TABLE t_part;
