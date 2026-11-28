-- Tags: no-parallel-replicas
-- `IfTransformStringsToEnumPass` replaces the string constants of `if` with folded
-- `_CAST(<string>, 'Enum8(...)')` constants, which are not in the stored key expression. They do not
-- depend on the settings, so a `minmax` skip index, the primary key and the partition key on such an
-- expression must still be used with `optimize_if_transform_strings_to_enum = 1`. Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_if_transform_strings_to_enum = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

CREATE TABLE t_idx (id UInt32, v Int32, INDEX idx if(v > 49, 'a', 'b') TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_idx SELECT number, number FROM numbers(100);

SELECT 'skip index';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_idx WHERE if(v > 49, 'a', 'b') = 'a')
WHERE explain LIKE '%Granules:%/%' OR explain LIKE '%Name:%';
SELECT count() FROM t_idx WHERE if(v > 49, 'a', 'b') = 'a';

CREATE TABLE t_pk (id UInt32, v Int32)
ENGINE = MergeTree ORDER BY if(v > 49, 'a', 'b')
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_pk SELECT number, number FROM numbers(100);

SELECT 'primary key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_pk WHERE if(v > 49, 'a', 'b') = 'a')
WHERE explain LIKE '%Granules:%/%';
SELECT count() FROM t_pk WHERE if(v > 49, 'a', 'b') = 'a';

CREATE TABLE t_part (id UInt32, v Int32)
ENGINE = MergeTree PARTITION BY if(v > 49, 'a', 'b') ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_part SELECT number, number FROM numbers(100);

SELECT 'partition key';
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_part WHERE if(v > 49, 'a', 'b') = 'a')
WHERE explain LIKE '%Parts:%/%';
SELECT count() FROM t_part WHERE if(v > 49, 'a', 'b') = 'a';

DROP TABLE t_idx;
DROP TABLE t_pk;
DROP TABLE t_part;
