-- Tags: no-parallel-replicas
-- `FunctionToSubcolumnsPass` does not rewrite `m['k']` to the subcolumn `m.key_k` when the query uses
-- `m` in another way (e.g. `mapKeys(m)`) or reads the table with `FINAL`. A key on
-- `multiIf(m['k'] > 0, m['k'], 0)` must still be matched by the filter, in which only `multiIf` is
-- rewritten to `if`. The decision is made per column: with two `Map` columns, `n['k']` is still
-- rewritten to `n.key_k` while `m['k']` is not. Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;
SET optimize_multiif_to_if = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_final;
DROP TABLE IF EXISTS t_idx;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_mixed_pk;
DROP TABLE IF EXISTS t_mixed_idx;
DROP TABLE IF EXISTS t_mixed_part;

CREATE TABLE t_pk (id UInt32, m Map(String, Int32))
ENGINE = MergeTree ORDER BY multiIf(m['k'] > 0, m['k'], 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_pk SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'primary key, subcolumn rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_pk WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%';
SELECT 'primary key, subcolumn not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT mapKeys(m) FROM t_pk WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%';
SELECT id, mapKeys(m) FROM t_pk WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 ORDER BY id;

CREATE TABLE t_final (id UInt32, m Map(String, Int32))
ENGINE = ReplacingMergeTree ORDER BY multiIf(m['k'] > 0, m['k'], 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_final SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'primary key, FINAL';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_final FINAL WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%';
SELECT id FROM t_final FINAL WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 ORDER BY id;

CREATE TABLE t_idx (id UInt32, m Map(String, Int32), INDEX idx multiIf(m['k'] > 0, m['k'], 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_idx SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'skip index, subcolumn not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT mapKeys(m) FROM t_idx WHERE multiIf(m['k'] > 0, m['k'], 0) > 97)
WHERE explain LIKE '%Granules:%/%' OR explain LIKE '%Name:%';

CREATE TABLE t_part (id UInt32, m Map(String, Int32))
ENGINE = MergeTree PARTITION BY multiIf(m['k'] > 49, 1, 0) ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_part SELECT number, map('k', toInt32(number)) FROM numbers(100);

SELECT 'partition key, subcolumn not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT mapKeys(m) FROM t_part WHERE multiIf(m['k'] > 49, 1, 0) = 1)
WHERE explain LIKE '%Parts:%/%';

CREATE TABLE t_mixed_pk (id UInt32, m Map(String, Int32), n Map(String, Int32))
ENGINE = MergeTree ORDER BY multiIf(m['k'] > 0, m['k'], 0) + multiIf(n['k'] > 0, n['k'], 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_mixed_pk SELECT number, map('k', toInt32(number)), map('k', toInt32(number)) FROM numbers(100);

SELECT 'primary key, subcolumn of one column not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT mapKeys(m) FROM t_mixed_pk WHERE multiIf(m['k'] > 0, m['k'], 0) + multiIf(n['k'] > 0, n['k'], 0) > 194)
WHERE explain LIKE '%Granules:%/%';
SELECT id, mapKeys(m) FROM t_mixed_pk WHERE multiIf(m['k'] > 0, m['k'], 0) + multiIf(n['k'] > 0, n['k'], 0) > 194 ORDER BY id;

CREATE TABLE t_mixed_idx (id UInt32, m Map(String, Int32), n Map(String, Int32),
    INDEX idx multiIf(m['k'] > 0, m['k'], 0) + multiIf(n['k'] > 0, n['k'], 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_mixed_idx SELECT number, map('k', toInt32(number)), map('k', toInt32(number)) FROM numbers(100);

SELECT 'skip index, subcolumn of one column not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT mapKeys(m) FROM t_mixed_idx WHERE multiIf(m['k'] > 0, m['k'], 0) + multiIf(n['k'] > 0, n['k'], 0) > 194)
WHERE explain LIKE '%Granules:%/%' OR explain LIKE '%Name:%';

CREATE TABLE t_mixed_part (id UInt32, m Map(String, Int32), n Map(String, Int32))
ENGINE = MergeTree PARTITION BY multiIf(m['k'] > 49, 1, 0) + multiIf(n['k'] > 49, 1, 0) ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_mixed_part SELECT number, map('k', toInt32(number)), map('k', toInt32(number)) FROM numbers(100);

SELECT 'partition key, subcolumn of one column not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT mapKeys(m) FROM t_mixed_part WHERE multiIf(m['k'] > 49, 1, 0) + multiIf(n['k'] > 49, 1, 0) = 2)
WHERE explain LIKE '%Parts:%/%';

DROP TABLE t_pk;
DROP TABLE t_final;
DROP TABLE t_idx;
DROP TABLE t_part;
DROP TABLE t_mixed_pk;
DROP TABLE t_mixed_idx;
DROP TABLE t_mixed_part;
