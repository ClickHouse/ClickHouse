-- A `mapKeys(m)` text index skips granules without the key only when `m['key']` alone decides the
-- predicate there. A predicate that also reads `m` elsewhere, e.g. `length(m)`, must not use it.

SET use_skip_indexes = 1;

DROP TABLE IF EXISTS t_basic;
DROP TABLE IF EXISTS t_buckets;
DROP TABLE IF EXISTS t_fs;
DROP TABLE IF EXISTS t_hint;

CREATE TABLE t_basic (id UInt8, m Map(String, UInt8), INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';

CREATE TABLE t_buckets (id UInt8, m Map(String, UInt8), INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';

CREATE TABLE t_fs (id UInt8, m Map(FixedString(4), UInt8), INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_basic VALUES (1, {'x': 2}), (2, {'ab': 0, 'y': 1}), (3, {}), (4, {'ab': 3});
INSERT INTO t_buckets VALUES (1, {'x': 2}), (2, {'ab': 0, 'y': 1}), (3, {}), (4, {'ab': 3});
INSERT INTO t_fs VALUES (1, {'x': 2}), (2, {});

SELECT '-- without the index';
SELECT 'lt', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] < length(m) SETTINGS use_skip_indexes = 0;
SELECT 'eq', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] + 1 = length(m) SETTINGS use_skip_indexes = 0;
SELECT 'if', arraySort(groupArray(id)) FROM t_basic WHERE if(length(m) > 0, m['ab'] = 0, 0) SETTINGS use_skip_indexes = 0;

SELECT '-- the map is read outside m[key]: no row without the key is skipped';
SELECT 'basic lt', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] < length(m) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'basic lt', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] < length(m) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'basic eq', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] + 1 = length(m) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'basic eq', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] + 1 = length(m) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'basic if', arraySort(groupArray(id)) FROM t_basic WHERE if(length(m) > 0, m['ab'] = 0, 0) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'basic if', arraySort(groupArray(id)) FROM t_basic WHERE if(length(m) > 0, m['ab'] = 0, 0) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'buckets lt', arraySort(groupArray(id)) FROM t_buckets WHERE m['ab'] < length(m) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'buckets lt', arraySort(groupArray(id)) FROM t_buckets WHERE m['ab'] < length(m) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'buckets eq', arraySort(groupArray(id)) FROM t_buckets WHERE m['ab'] + 1 = length(m) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'buckets eq', arraySort(groupArray(id)) FROM t_buckets WHERE m['ab'] + 1 = length(m) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'buckets if', arraySort(groupArray(id)) FROM t_buckets WHERE if(length(m) > 0, m['ab'] = 0, 0) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'buckets if', arraySort(groupArray(id)) FROM t_buckets WHERE if(length(m) > 0, m['ab'] = 0, 0) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'fixedstring lt', arraySort(groupArray(id)) FROM t_fs WHERE m['ab'] < length(m);
SELECT 'not used', count() FROM (EXPLAIN indexes = 1 SELECT id FROM t_basic WHERE m['ab'] < length(m) SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%idx_keys%';

SELECT '-- m[key] alone, or another use of the map in a separate conjunct, still prunes';
SELECT 'gt', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] > 0 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'gt and length', arraySort(groupArray(id)) FROM t_basic WHERE m['ab'] > 0 AND length(m) = 1 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'gt prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_basic WHERE m['ab'] > 0 SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%Granules: 2/4%';
SELECT 'gt and length prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM t_basic WHERE m['ab'] > 0 AND length(m) = 1 SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain ILIKE '%Granules: 2/4%';

SELECT '-- a hint from another conjunct keeps the value of m[key] in an atom that does not use the index';
CREATE TABLE t_hint (id UInt8, m Map(String, UInt8), INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO t_hint VALUES (1, {'x': 2}), (2, {'ab': 2, 'y': 1}), (3, {'ab': 1});
SELECT 'hint truth', arraySort(groupArray(id)) FROM t_hint WHERE m['ab'] < length(m) AND m['ab'] > 0 SETTINGS use_skip_indexes = 0;
SELECT 'hint', arraySort(groupArray(id)) FROM t_hint WHERE m['ab'] < length(m) AND m['ab'] > 0 SETTINGS optimize_functions_to_subcolumns = 0, query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, text_index_hint_max_selectivity = 1;
SELECT 'hint', arraySort(groupArray(id)) FROM t_hint WHERE m['ab'] < length(m) AND m['ab'] > 0 SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1, text_index_hint_max_selectivity = 1;

DROP TABLE t_basic;
DROP TABLE t_buckets;
DROP TABLE t_fs;
DROP TABLE t_hint;
