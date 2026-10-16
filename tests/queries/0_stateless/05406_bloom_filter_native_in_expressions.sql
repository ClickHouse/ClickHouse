SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 0;
SET input_format_values_interpret_expressions = 1;

DROP TABLE IF EXISTS t_bf_post5_types;
DROP TABLE IF EXISTS t_bf_post5_map_expr;
DROP TABLE IF EXISTS t_bf_post5_array_expr;
DROP TABLE IF EXISTS t_bf_post5_map_collision;
DROP TABLE IF EXISTS t_bf_post5_array_collision;

CREATE TABLE t_bf_post5_types
(
    id UInt64,
    flag Bool,
    flag_u8 UInt8,
    INDEX idx_bool flag TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_u8 flag_u8 TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_post5_types VALUES
(0, false, 0), (1, false, 0), (2, false, 0), (3, true, 1),
(4, false, 0), (5, false, 0), (6, false, 0), (7, false, 0),
(8, false, 0), (9, false, 0), (10, false, 0), (11, false, 0),
(12, false, 0), (13, false, 0), (14, false, 0), (15, false, 0);

-- Positive controls: index the complete native expressions, without same-named physical columns.
CREATE TABLE t_bf_post5_map_expr
(
    id UInt64,
    m Map(String, UInt64),
    INDEX idx_map_keys mapKeys(m) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_post5_map_expr VALUES
(0, map('k0', toUInt64(0))), (1, map('k1', toUInt64(1))),
(2, map('k2', toUInt64(2))), (3, map('3', toUInt64(3))),
(4, map('k4', toUInt64(4))), (5, map('k5', toUInt64(5))),
(6, map('k6', toUInt64(6))), (7, map('k7', toUInt64(7))),
(8, map('k8', toUInt64(8))), (9, map('k9', toUInt64(9))),
(10, map('k10', toUInt64(10))), (11, map('k11', toUInt64(11))),
(12, map('k12', toUInt64(12))), (13, map('k13', toUInt64(13))),
(14, map('k14', toUInt64(14))), (15, map('k15', toUInt64(15)));

CREATE TABLE t_bf_post5_array_expr
(
    id UInt64,
    a Array(String),
    INDEX idx_array_sort arraySort(a) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_array_reverse arrayReverse(a) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_post5_array_expr VALUES
(0, ['k0']), (1, ['k1']), (2, ['k2']), (3, ['3']),
(4, ['k4']), (5, ['k5']), (6, ['k6']), (7, ['k7']),
(8, ['k8']), (9, ['k9']), (10, ['k10']), (11, ['k11']),
(12, ['k12']), (13, ['k13']), (14, ['k14']), (15, ['k15']);

-- Collision controls: index only the physical column whose display name matches a function.
-- The computed function yields '3' on id 3; the quoted physical column yields it on id 4.
CREATE TABLE t_bf_post5_map_collision
(
    id UInt64,
    m Map(String, UInt64),
    `mapKeys(m)` Array(String),
    INDEX idx_map_keys_physical `mapKeys(m)` TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_post5_map_collision VALUES
(0, map('k0', toUInt64(0)), CAST([], 'Array(String)')),
(1, map('k1', toUInt64(1)), CAST([], 'Array(String)')),
(2, map('k2', toUInt64(2)), CAST([], 'Array(String)')),
(3, map('3', toUInt64(3)), CAST([], 'Array(String)')),
(4, map('k4', toUInt64(4)), ['3']),
(5, map('k5', toUInt64(5)), CAST([], 'Array(String)')),
(6, map('k6', toUInt64(6)), CAST([], 'Array(String)')),
(7, map('k7', toUInt64(7)), CAST([], 'Array(String)')),
(8, map('k8', toUInt64(8)), CAST([], 'Array(String)')),
(9, map('k9', toUInt64(9)), CAST([], 'Array(String)')),
(10, map('k10', toUInt64(10)), CAST([], 'Array(String)')),
(11, map('k11', toUInt64(11)), CAST([], 'Array(String)')),
(12, map('k12', toUInt64(12)), CAST([], 'Array(String)')),
(13, map('k13', toUInt64(13)), CAST([], 'Array(String)')),
(14, map('k14', toUInt64(14)), CAST([], 'Array(String)')),
(15, map('k15', toUInt64(15)), CAST([], 'Array(String)'));

CREATE TABLE t_bf_post5_array_collision
(
    id UInt64,
    a Array(String),
    `arraySort(a)` Array(String),
    INDEX idx_array_sort_physical `arraySort(a)` TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_post5_array_collision VALUES
(0, ['k0'], CAST([], 'Array(String)')),
(1, ['k1'], CAST([], 'Array(String)')),
(2, ['k2'], CAST([], 'Array(String)')),
(3, ['3'], CAST([], 'Array(String)')),
(4, ['k4'], ['3']),
(5, ['k5'], CAST([], 'Array(String)')),
(6, ['k6'], CAST([], 'Array(String)')),
(7, ['k7'], CAST([], 'Array(String)')),
(8, ['k8'], CAST([], 'Array(String)')),
(9, ['k9'], CAST([], 'Array(String)')),
(10, ['k10'], CAST([], 'Array(String)')),
(11, ['k11'], CAST([], 'Array(String)')),
(12, ['k12'], CAST([], 'Array(String)')),
(13, ['k13'], CAST([], 'Array(String)')),
(14, ['k14'], CAST([], 'Array(String)')),
(15, ['k15'], CAST([], 'Array(String)'));

-- Complete native expressions should select one granule each.
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_types WHERE flag IN (true))
WHERE explain LIKE '%Name: idx_bool%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_types WHERE flag_u8 IN (1))
WHERE explain LIKE '%Name: idx_u8%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_map_expr WHERE arrayJoin(mapKeys(m)) IN ('3'))
WHERE explain LIKE '%Name: idx_map_keys%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_array_expr WHERE arrayJoin(arraySort(a)) IN ('3'))
WHERE explain LIKE '%Name: idx_array_sort%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_array_expr WHERE arrayJoin(arrayReverse(a)) IN ('3'))
WHERE explain LIKE '%Name: idx_array_reverse%' OR explain LIKE '%Granules:%';

-- A physical same-named column index must not satisfy the computed expression.
SELECT toUInt8(max(explain LIKE '%Name: idx_map_keys_physical%') = 0)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_map_collision WHERE arrayJoin(mapKeys(m)) IN ('3'));
SELECT toUInt8(max(explain LIKE '%Name: idx_array_sort_physical%') = 0)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_array_collision WHERE arrayJoin(arraySort(a)) IN ('3'));

-- Quoted physical inputs may use their own physical-column indexes.
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_map_collision WHERE arrayJoin(`mapKeys(m)`) IN ('3'))
WHERE explain LIKE '%Name: idx_map_keys_physical%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_array_collision WHERE arrayJoin(`arraySort(a)`) IN ('3'))
WHERE explain LIKE '%Name: idx_array_sort_physical%' OR explain LIKE '%Granules:%';

-- Equality and direct-IN stay positive controls for expression indexes.
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_map_expr WHERE arrayJoin(mapKeys(m)) = '3')
WHERE explain LIKE '%Name: idx_map_keys%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_array_expr WHERE arrayJoin(arraySort(a)) = '3')
WHERE explain LIKE '%Name: idx_array_sort%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_post5_array_expr WHERE arraySort(a) IN (['3']))
WHERE explain LIKE '%Name: idx_array_sort%' OR explain LIKE '%Granules:%';

-- Read back the physical collision values, then compare indexed/full-scan semantics.
SELECT 'map physical', id, `mapKeys(m)` FROM t_bf_post5_map_collision WHERE id IN (3, 4) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'array physical', id, `arraySort(a)` FROM t_bf_post5_array_collision WHERE id IN (3, 4) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'Bool indexed', arraySort(groupArray(id)) FROM t_bf_post5_types WHERE flag IN (true);
SELECT 'Bool full scan', arraySort(groupArray(id)) FROM t_bf_post5_types WHERE flag IN (true) SETTINGS use_skip_indexes = 0;
SELECT 'UInt8 indexed', arraySort(groupArray(id)) FROM t_bf_post5_types WHERE flag_u8 IN (1);
SELECT 'UInt8 full scan', arraySort(groupArray(id)) FROM t_bf_post5_types WHERE flag_u8 IN (1) SETTINGS use_skip_indexes = 0;
SELECT 'map indexed', arraySort(groupArray(id)) FROM t_bf_post5_map_expr WHERE arrayJoin(mapKeys(m)) IN ('3');
SELECT 'map full scan', arraySort(groupArray(id)) FROM t_bf_post5_map_expr WHERE arrayJoin(mapKeys(m)) IN ('3') SETTINGS use_skip_indexes = 0;
SELECT 'map computed collision', arraySort(groupArray(id)) FROM t_bf_post5_map_collision WHERE arrayJoin(mapKeys(m)) IN ('3');
SELECT 'map computed collision full scan', arraySort(groupArray(id)) FROM t_bf_post5_map_collision WHERE arrayJoin(mapKeys(m)) IN ('3') SETTINGS use_skip_indexes = 0;
SELECT 'map physical collision', arraySort(groupArray(id)) FROM t_bf_post5_map_collision WHERE arrayJoin(`mapKeys(m)`) IN ('3');
SELECT 'array indexed', arraySort(groupArray(id)) FROM t_bf_post5_array_expr WHERE arrayJoin(arraySort(a)) IN ('3');
SELECT 'array full scan', arraySort(groupArray(id)) FROM t_bf_post5_array_expr WHERE arrayJoin(arraySort(a)) IN ('3') SETTINGS use_skip_indexes = 0;
SELECT 'array computed collision', arraySort(groupArray(id)) FROM t_bf_post5_array_collision WHERE arrayJoin(arraySort(a)) IN ('3');
SELECT 'array computed collision full scan', arraySort(groupArray(id)) FROM t_bf_post5_array_collision WHERE arrayJoin(arraySort(a)) IN ('3') SETTINGS use_skip_indexes = 0;
SELECT 'array physical collision', arraySort(groupArray(id)) FROM t_bf_post5_array_collision WHERE arrayJoin(`arraySort(a)`) IN ('3');
SELECT 'arrayReverse indexed', arraySort(groupArray(id)) FROM t_bf_post5_array_expr WHERE arrayJoin(arrayReverse(a)) IN ('3');
SELECT 'arrayReverse full scan', arraySort(groupArray(id)) FROM t_bf_post5_array_expr WHERE arrayJoin(arrayReverse(a)) IN ('3') SETTINGS use_skip_indexes = 0;

DROP TABLE t_bf_post5_types;
DROP TABLE t_bf_post5_map_expr;
DROP TABLE t_bf_post5_array_expr;
DROP TABLE t_bf_post5_map_collision;
DROP TABLE t_bf_post5_array_collision;
