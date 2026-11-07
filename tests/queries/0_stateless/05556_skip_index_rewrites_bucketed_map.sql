-- Tags: no-parallel-replicas
-- `FunctionToSubcolumnsPass` rewrites `m['k']` to the subcolumn `m.key_k` only for a table with
-- bucketed map serialization. The alternative form of a key on `multiIf(m['k'] > 0, m['k'], 0)` must
-- follow the query: with buckets the filter reads `m.key_k`, without buckets it reads `m['k']`, and the
-- primary key, the `minmax` skip index and the partition key must be used in both cases.
-- Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;
SET optimize_multiif_to_if = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_pk_buckets;
DROP TABLE IF EXISTS t_pk_basic;
DROP TABLE IF EXISTS t_idx_buckets;
DROP TABLE IF EXISTS t_idx_basic;
DROP TABLE IF EXISTS t_part_buckets;
DROP TABLE IF EXISTS t_part_basic;

CREATE TABLE t_pk_buckets (id UInt32, m Map(String, Int32))
ENGINE = MergeTree ORDER BY multiIf(m['k'] > 0, m['k'], 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_pk_buckets SELECT number, map('k', toInt32(number)) FROM numbers(100);

CREATE TABLE t_pk_basic (id UInt32, m Map(String, Int32))
ENGINE = MergeTree ORDER BY multiIf(m['k'] > 0, m['k'], 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO t_pk_basic SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'primary key, with buckets';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_pk_buckets WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%';
SELECT id FROM t_pk_buckets WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 ORDER BY id;
SELECT 'primary key, basic';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_pk_basic WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%';
SELECT id FROM t_pk_basic WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 ORDER BY id;

CREATE TABLE t_idx_buckets (id UInt32, m Map(String, Int32), INDEX idx multiIf(m['k'] > 0, m['k'], 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_idx_buckets SELECT number, map('k', toInt32(number)) FROM numbers(100);

CREATE TABLE t_idx_basic (id UInt32, m Map(String, Int32), INDEX idx multiIf(m['k'] > 0, m['k'], 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO t_idx_basic SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'skip index, with buckets';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_idx_buckets WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%' OR explain LIKE '%Name:%';
SELECT 'skip index, basic';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_idx_basic WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%' OR explain LIKE '%Name:%';

CREATE TABLE t_part_buckets (id UInt32, m Map(String, Int32))
ENGINE = MergeTree PARTITION BY multiIf(m['k'] > 49, 1, 0) ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_part_buckets SELECT number, map('k', toInt32(number)) FROM numbers(100);

CREATE TABLE t_part_basic (id UInt32, m Map(String, Int32))
ENGINE = MergeTree PARTITION BY multiIf(m['k'] > 49, 1, 0) ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0,
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO t_part_basic SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'partition key, with buckets';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_part_buckets WHERE multiIf(m['k'] > 49, 1, 0) = 1)
WHERE explain LIKE '%Parts:%/%';
SELECT count() FROM t_part_buckets WHERE multiIf(m['k'] > 49, 1, 0) = 1;
SELECT 'partition key, basic';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_part_basic WHERE multiIf(m['k'] > 49, 1, 0) = 1)
WHERE explain LIKE '%Parts:%/%';
SELECT count() FROM t_part_basic WHERE multiIf(m['k'] > 49, 1, 0) = 1;

DROP TABLE t_pk_buckets;
DROP TABLE t_pk_basic;
DROP TABLE t_idx_buckets;
DROP TABLE t_idx_basic;
DROP TABLE t_part_buckets;
DROP TABLE t_part_basic;
