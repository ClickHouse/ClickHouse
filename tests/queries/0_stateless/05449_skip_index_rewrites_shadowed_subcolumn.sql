-- A key on `m['k']` must not be matched by the name of a physical column `m.key_k`.
-- `FunctionToSubcolumnsPass` rewrites `m['k']` to the subcolumn `m.key_k`, but not when the table
-- has a column of that name, so in such a table `m.key_k` refers to the other column, not to the key.
-- Related to issue #103128.

SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_shadow_pk;
DROP TABLE IF EXISTS t_shadow_idx;
DROP TABLE IF EXISTS t_shadow_part;

CREATE TABLE t_shadow_pk (m Map(String, Int32), `m.key_k` Int32)
ENGINE = MergeTree ORDER BY m['k']
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_shadow_pk SELECT map('k', toInt32(100 + number)), number FROM numbers(100);

SELECT count() FROM t_shadow_pk WHERE `m.key_k` = 99;
SELECT count() FROM t_shadow_pk WHERE `m.key_k` = 99 SETTINGS use_primary_key = 0;

CREATE TABLE t_shadow_idx (m Map(String, Int32), `m.key_k` Int32, INDEX idx m['k'] TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_shadow_idx SELECT map('k', toInt32(100 + number)), number FROM numbers(100);

SELECT count() FROM t_shadow_idx WHERE `m.key_k` = 99;
SELECT count() FROM t_shadow_idx WHERE `m.key_k` = 99 SETTINGS use_skip_indexes = 0;

CREATE TABLE t_shadow_part (m Map(String, Int32), `m.key_k` Int32)
ENGINE = MergeTree PARTITION BY m['k'] ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_shadow_part SELECT map('k', toInt32(100 + number)), number FROM numbers(10);

SELECT count() FROM t_shadow_part WHERE `m.key_k` = 9;
SELECT count() FROM t_shadow_part WHERE `m.key_k` = 9 SETTINGS use_partition_pruning = 0;

-- Partition pruning of a mutation.
ALTER TABLE t_shadow_part DELETE WHERE `m.key_k` = 9 SETTINGS mutations_sync = 2;
SELECT count() FROM t_shadow_part WHERE `m.key_k` = 9 SETTINGS use_partition_pruning = 0;

-- Without the shadowing column, the subcolumn filter still uses the key.
DROP TABLE IF EXISTS t_no_shadow;
CREATE TABLE t_no_shadow (m Map(String, Int32))
ENGINE = MergeTree ORDER BY m['k']
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_no_shadow SELECT map('k', toInt32(100 + number)) FROM numbers(100);
SELECT count() FROM t_no_shadow WHERE m['k'] = 150 SETTINGS force_primary_key = 1;

-- A mixed key: the shadowed subcolumn rewrite of `m['k']` is not applied, but the rewrite of `multiIf`
-- to `if` still is, so the key is still used.
DROP TABLE IF EXISTS t_shadow_mixed_pk;
DROP TABLE IF EXISTS t_shadow_mixed_idx;
DROP TABLE IF EXISTS t_shadow_mixed_part;

CREATE TABLE t_shadow_mixed_pk (id UInt32, m Map(String, Int32), `m.key_k` Int32)
ENGINE = MergeTree ORDER BY multiIf(m['k'] > 0, m['k'], 0)
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_shadow_mixed_pk SELECT number, map('k', toInt32(number)), 100 + number FROM numbers(100);
SELECT count() FROM t_shadow_mixed_pk WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 SETTINGS optimize_multiif_to_if = 1, force_primary_key = 1;
SELECT count() FROM t_shadow_mixed_pk WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 SETTINGS optimize_multiif_to_if = 1, use_primary_key = 0;
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_shadow_mixed_pk WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 SETTINGS optimize_multiif_to_if = 1)
WHERE explain LIKE '%Granules%';

CREATE TABLE t_shadow_mixed_idx (id UInt32, m Map(String, Int32), `m.key_k` Int32, INDEX idx multiIf(m['k'] > 0, m['k'], 0) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_shadow_mixed_idx SELECT number, map('k', toInt32(number)), 100 + number FROM numbers(100);
SELECT count() FROM t_shadow_mixed_idx WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 SETTINGS optimize_multiif_to_if = 1, force_data_skipping_indices = 'idx';
SELECT count() FROM t_shadow_mixed_idx WHERE multiIf(m['k'] > 0, m['k'], 0) > 97 SETTINGS optimize_multiif_to_if = 1, use_skip_indexes = 0;

CREATE TABLE t_shadow_mixed_part (id UInt32, m Map(String, Int32), `m.key_k` Int32)
ENGINE = MergeTree PARTITION BY multiIf(m['k'] > 49, 1, 0) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_shadow_mixed_part SELECT number, map('k', toInt32(number)), 100 + number FROM numbers(100);
SELECT count() FROM t_shadow_mixed_part WHERE multiIf(m['k'] > 49, 1, 0) = 1 SETTINGS optimize_multiif_to_if = 1, force_index_by_date = 0;
SELECT replaceRegexpAll(explain, '^[│ ]+', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_shadow_mixed_part WHERE multiIf(m['k'] > 49, 1, 0) = 1 SETTINGS optimize_multiif_to_if = 1)
WHERE explain LIKE '%Parts:%';

DROP TABLE t_shadow_mixed_pk;
DROP TABLE t_shadow_mixed_idx;
DROP TABLE t_shadow_mixed_part;

DROP TABLE t_shadow_pk;
DROP TABLE t_shadow_idx;
DROP TABLE t_shadow_part;
DROP TABLE t_no_shadow;
