SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 0;
SET input_format_values_interpret_expressions = 1;

DROP TABLE IF EXISTS t_bf_post5_array_direct_in;
DROP TABLE IF EXISTS t_bf_post5_decimal_expr;
DROP TABLE IF EXISTS t_bf_post5_types;
DROP TABLE IF EXISTS t_bf_post5_map_expr;
DROP TABLE IF EXISTS t_bf_post5_array_expr;
DROP TABLE IF EXISTS t_bf_post5_map_collision;
DROP TABLE IF EXISTS t_bf_post5_array_collision;
DROP TABLE IF EXISTS t_bf_post5_intermediate_expr;
DROP TABLE IF EXISTS t_bf_post5_array_slice_mismatch;

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

CREATE TABLE t_bf_post5_intermediate_expr
(
    id UInt64,
    x UInt64,
    a Array(UInt64),
    INDEX idx_tuple_hash cityHash64(tuple(x, x)) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_array_slice arraySlice(a, 1) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_array_resize arrayResize(a, 2) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_post5_intermediate_expr
SELECT toUInt64(number + 1),
       if(number = 2, toUInt64(103), toUInt64(1000 + number)),
       if(number = 2,
          [toUInt64(103), toUInt64(203)],
          [toUInt64(1000 + number), toUInt64(2000 + number)])
FROM numbers(16);

CREATE TABLE t_bf_post5_array_slice_mismatch
(
    id UInt64,
    x UInt64,
    a Array(UInt64),
    INDEX idx_array_slice_second arraySlice(a, 2) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_post5_array_slice_mismatch
SELECT toUInt64(number + 1),
       if(number = 2, toUInt64(103), toUInt64(1000 + number)),
       if(number = 2,
          [toUInt64(103), toUInt64(203)],
          [toUInt64(1000 + number), toUInt64(2000 + number)])
FROM numbers(16);

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

-- Separately constructed tuple types and exact numeric arguments remain matchable through expression intermediates.
SELECT 'tuple hash IN full scan preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE cityHash64(tuple(x, x)) IN (cityHash64(tuple(toUInt64(103), toUInt64(103))))
SETTINGS use_skip_indexes = 0;
SELECT 'tuple hash IN indexed read preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE cityHash64(tuple(x, x)) IN (cityHash64(tuple(toUInt64(103), toUInt64(103))))
SETTINGS use_skip_indexes = 1;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tuple_hash') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'tuple hash IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_intermediate_expr
      WHERE cityHash64(tuple(x, x)) IN (cityHash64(tuple(toUInt64(103), toUInt64(103)))));

-- A different tuple child expression must decline the index without losing the exact matching row.
SELECT 'tuple hash child expression mismatch full scan preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE cityHash64(tuple(x, x + toUInt64(1))) IN (cityHash64(tuple(toUInt64(103), toUInt64(104))))
SETTINGS use_skip_indexes = 0;
SELECT 'tuple hash child expression mismatch indexed read preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE cityHash64(tuple(x, x + toUInt64(1))) IN (cityHash64(tuple(toUInt64(103), toUInt64(104))))
SETTINGS use_skip_indexes = 1;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_tuple_hash') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'tuple hash child expression mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_intermediate_expr
      WHERE cityHash64(tuple(x, x + toUInt64(1))) IN (cityHash64(tuple(toUInt64(103), toUInt64(104)))));

SELECT 'arraySlice IN full scan preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE arrayJoin(arraySlice(a, 1)) IN (toUInt64(103))
SETTINGS use_skip_indexes = 0;
SELECT 'arraySlice IN indexed read preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE arrayJoin(arraySlice(a, 1)) IN (toUInt64(103))
SETTINGS use_skip_indexes = 1;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_slice') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arraySlice IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_intermediate_expr
      WHERE arrayJoin(arraySlice(a, 1)) IN (toUInt64(103))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1);

-- A different numeric slice offset must decline the index that contains only the second slice.
SELECT 'arraySlice offset mismatch full scan preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_array_slice_mismatch
WHERE arrayJoin(arraySlice(a, 1)) IN (toUInt64(103))
SETTINGS use_skip_indexes = 0;
SELECT 'arraySlice offset mismatch indexed read preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_array_slice_mismatch
WHERE arrayJoin(arraySlice(a, 1)) IN (toUInt64(103))
SETTINGS use_skip_indexes = 1;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_slice_second') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arraySlice offset mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_array_slice_mismatch
      WHERE arrayJoin(arraySlice(a, 1)) IN (toUInt64(103))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1);

SELECT 'arrayResize IN full scan preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE arrayJoin(arrayResize(a, 2)) IN (toUInt64(103))
SETTINGS use_skip_indexes = 0;
SELECT 'arrayResize IN indexed read preserves typed rows',
       arraySort(groupArray((toUInt64(id), toUInt64(x)))) = [(toUInt64(3), toUInt64(103))]
FROM t_bf_post5_intermediate_expr
WHERE arrayJoin(arrayResize(a, 2)) IN (toUInt64(103))
SETTINGS use_skip_indexes = 1;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_resize') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, name_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arrayResize IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_intermediate_expr
      WHERE arrayJoin(arrayResize(a, 2)) IN (toUInt64(103))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1);

-- The numeric length mismatch must not use the shorter expression index: the extended zero matches all rows.
SELECT 'arrayResize length mismatch full scan preserves every typed row',
       arraySort(groupArray((toUInt64(id), toUInt64(x))))
           = arrayMap(id -> (toUInt64(id), toUInt64(if(id = 3, 103, 999 + id))), range(1, 17))
FROM t_bf_post5_intermediate_expr
WHERE arrayJoin(arrayResize(a, 3)) IN (toUInt64(0))
SETTINGS use_skip_indexes = 0;
SELECT 'arrayResize length mismatch indexed read preserves every typed row',
       arraySort(groupArray((toUInt64(id), toUInt64(x))))
           = arrayMap(id -> (toUInt64(id), toUInt64(if(id = 3, 103, 999 + id))), range(1, 17))
FROM t_bf_post5_intermediate_expr
WHERE arrayJoin(arrayResize(a, 3)) IN (toUInt64(0))
SETTINGS use_skip_indexes = 1;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_resize') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arrayResize length mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_intermediate_expr
      WHERE arrayJoin(arrayResize(a, 3)) IN (toUInt64(0))
      SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
               query_plan_fuse_filter_into_array_join = 1);

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

-- DirectIN constant arrays use the same bounded fixed-width comparison as arrayJoin wrappers.
CREATE TABLE t_bf_post5_array_direct_in
(
    id UInt64,
    a Array(UInt8),
    INDEX idx_array_intersect arrayIntersect(a, [3]) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_post5_array_direct_in SELECT number, [number] FROM numbers(16);

SELECT 'array DirectIN indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), a))) = [(toUInt64(3), [toUInt8(3)])]
FROM t_bf_post5_array_direct_in WHERE arrayIntersect(a, [3]) IN ([3]);
SELECT 'array DirectIN full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), a))) = [(toUInt64(3), [toUInt8(3)])]
FROM t_bf_post5_array_direct_in WHERE arrayIntersect(a, [3]) IN ([3]) SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_intersect') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'array DirectIN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_array_direct_in WHERE arrayIntersect(a, [3]) IN ([3]));

SELECT 'arrayJoin control indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), a))) = [(toUInt64(3), [toUInt8(3)])]
FROM t_bf_post5_array_direct_in WHERE arrayJoin(arrayIntersect(a, [3])) IN (3);
SELECT 'arrayJoin control full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), a))) = [(toUInt64(3), [toUInt8(3)])]
FROM t_bf_post5_array_direct_in WHERE arrayJoin(arrayIntersect(a, [3])) IN (3) SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_intersect') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arrayJoin control retains index pruning 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_array_direct_in WHERE arrayJoin(arrayIntersect(a, [3])) IN (3));

SELECT 'unequal array literal indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), a))) = [(toUInt64(4), [toUInt8(4)])]
FROM t_bf_post5_array_direct_in WHERE arrayIntersect(a, [4]) IN ([4]);
SELECT 'unequal array literal full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), a))) = [(toUInt64(4), [toUInt8(4)])]
FROM t_bf_post5_array_direct_in WHERE arrayIntersect(a, [4]) IN ([4]) SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_intersect') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'unequal array literal declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_array_direct_in WHERE arrayIntersect(a, [4]) IN ([4]));

-- Build the index under one parsing context and query it under another below.
SET input_format_parallel_parsing = 1;
CREATE TABLE t_bf_post5_decimal_expr
(
    id UInt64,
    x UInt64,
    INDEX idx_decimal32 toString(toDecimal32(x, 2)) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_decimal64 toString(toDecimal64(x, 2)) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_decimal128 toString(toDecimal128(x, 2)) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_decimal256 toString(toDecimal256(x, 2)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_post5_decimal_expr SELECT number, number FROM numbers(16);

SELECT 'Decimal32 IN indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal32(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal32(x, 2)) IN (toString(toDecimal32(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0;
SELECT 'Decimal32 IN full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal32(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal32(x, 2)) IN (toString(toDecimal32(toUInt64(3), 2))) SETTINGS use_skip_indexes = 0, input_format_parallel_parsing = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal32') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal32 IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal32(x, 2)) IN (toString(toDecimal32(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0);
SELECT 'Decimal32 scale mismatch indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal32(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal32(x, 3)) IN (toString(toDecimal32(toUInt64(3), 3)));
SELECT 'Decimal32 scale mismatch full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal32(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal32(x, 3)) IN (toString(toDecimal32(toUInt64(3), 3))) SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal32') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal32 scale mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal32(x, 3)) IN (toString(toDecimal32(toUInt64(3), 3))));

SELECT 'Decimal64 IN indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal64(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal64(x, 2)) IN (toString(toDecimal64(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0;
SELECT 'Decimal64 IN full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal64(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal64(x, 2)) IN (toString(toDecimal64(toUInt64(3), 2))) SETTINGS use_skip_indexes = 0, input_format_parallel_parsing = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal64') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal64 IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal64(x, 2)) IN (toString(toDecimal64(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0);
SELECT 'Decimal64 scale mismatch indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal64(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal64(x, 3)) IN (toString(toDecimal64(toUInt64(3), 3)));
SELECT 'Decimal64 scale mismatch full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal64(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal64(x, 3)) IN (toString(toDecimal64(toUInt64(3), 3))) SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal64') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal64 scale mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal64(x, 3)) IN (toString(toDecimal64(toUInt64(3), 3))));

SELECT 'Decimal128 IN indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal128(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal128(x, 2)) IN (toString(toDecimal128(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0;
SELECT 'Decimal128 IN full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal128(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal128(x, 2)) IN (toString(toDecimal128(toUInt64(3), 2))) SETTINGS use_skip_indexes = 0, input_format_parallel_parsing = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal128') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal128 IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal128(x, 2)) IN (toString(toDecimal128(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0);
SELECT 'Decimal128 scale mismatch indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal128(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal128(x, 3)) IN (toString(toDecimal128(toUInt64(3), 3)));
SELECT 'Decimal128 scale mismatch full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal128(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal128(x, 3)) IN (toString(toDecimal128(toUInt64(3), 3))) SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal128') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal128 scale mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal128(x, 3)) IN (toString(toDecimal128(toUInt64(3), 3))));

SELECT 'Decimal256 IN indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal256(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal256(x, 2)) IN (toString(toDecimal256(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0;
SELECT 'Decimal256 IN full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal256(x, 2))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal256(x, 2)) IN (toString(toDecimal256(toUInt64(3), 2))) SETTINGS use_skip_indexes = 0, input_format_parallel_parsing = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal256') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal256 IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal256(x, 2)) IN (toString(toDecimal256(toUInt64(3), 2))) SETTINGS input_format_parallel_parsing = 0);
SELECT 'Decimal256 scale mismatch indexed rows preserve typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal256(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal256(x, 3)) IN (toString(toDecimal256(toUInt64(3), 3)));
SELECT 'Decimal256 scale mismatch full scan preserves typed values',
       arraySort(groupArray((toUInt64(id), toUInt64(x), toString(toDecimal256(x, 3))))) = [(toUInt64(3), toUInt64(3), '3')]
FROM t_bf_post5_decimal_expr WHERE toString(toDecimal256(x, 3)) IN (toString(toDecimal256(toUInt64(3), 3))) SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_decimal256') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Decimal256 scale mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_post5_decimal_expr WHERE toString(toDecimal256(x, 3)) IN (toString(toDecimal256(toUInt64(3), 3))));

DROP TABLE t_bf_post5_types;
DROP TABLE t_bf_post5_map_expr;
DROP TABLE t_bf_post5_array_expr;
DROP TABLE t_bf_post5_map_collision;
DROP TABLE t_bf_post5_array_collision;
DROP TABLE t_bf_post5_intermediate_expr;
DROP TABLE t_bf_post5_array_slice_mismatch;
DROP TABLE t_bf_post5_array_direct_in;
DROP TABLE t_bf_post5_decimal_expr;
-- Guard native conversion matching across irrelevant and value-affecting settings.
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET input_format_parallel_parsing = 1;
SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;

CREATE TABLE t_bf_conversion_numeric_wide
(
    id UInt64,
    u UInt64,
    x Int256,
    f Float64,
    INDEX idx_uint64 toString(u) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_int256 toString(x) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_float64 toString(f) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_conversion_numeric_wide
SELECT number, number, toInt256(number), toFloat64(number) FROM numbers(16);

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_uint64') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt64 toString IN changed unrelated setting uses index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_conversion_numeric_wide WHERE toString(u) IN ('3') SETTINGS input_format_parallel_parsing = 0);
SELECT 'UInt64 toString IN changed unrelated setting preserves row', arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM t_bf_conversion_numeric_wide WHERE toString(u) IN ('3') SETTINGS input_format_parallel_parsing = 0;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_int256') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Int256 toString IN changed unrelated setting uses index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_conversion_numeric_wide WHERE toString(x) IN ('3') SETTINGS input_format_parallel_parsing = 0);
SELECT 'Int256 toString IN changed unrelated setting preserves row', arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM t_bf_conversion_numeric_wide WHERE toString(x) IN ('3') SETTINGS input_format_parallel_parsing = 0;

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_float64') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Float64 toString IN changed unrelated setting uses index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_conversion_numeric_wide WHERE toString(f) IN ('3') SETTINGS input_format_parallel_parsing = 0);
SELECT 'Float64 toString IN changed unrelated setting preserves row', arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM t_bf_conversion_numeric_wide WHERE toString(f) IN ('3') SETTINGS input_format_parallel_parsing = 0;

CREATE TABLE t_bf_conversion_datetime64
(
    id UInt64,
    dt DateTime64(6),
    INDEX idx_datetime64 toString(dt) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_conversion_datetime64
SELECT number, toDateTime64('2020-01-01 00:00:00.123000', 6) + toIntervalSecond(toInt64(number))
FROM numbers(16);
SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1;
SELECT 'DateTime64 effective setting changes toString output', toString(dt) = '2020-01-01 00:00:00.123'
FROM t_bf_conversion_datetime64 WHERE id = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_datetime64') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'DateTime64 effective format mismatch declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_conversion_datetime64 WHERE toString(dt) IN ('2020-01-01 00:00:00.123'));
SELECT 'DateTime64 effective format mismatch preserves row', arraySort(groupArray(toUInt64(id))) = [toUInt64(0)]
FROM t_bf_conversion_datetime64 WHERE toString(dt) IN ('2020-01-01 00:00:00.123');

CREATE TABLE t_bf_conversion_nullable
(
    id UInt64,
    s String,
    INDEX idx_nullable_if if(id % 2, s, NULL) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_conversion_nullable SELECT number, toString(number) FROM numbers(16);
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nullable_if') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Nullable if direct IN uses index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_conversion_nullable WHERE if(id % 2, s, NULL) IN ('3'));
SELECT 'Nullable if direct IN preserves row', arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM t_bf_conversion_nullable WHERE if(id % 2, s, NULL) IN ('3');
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nullable_if') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Nullable if equality control uses index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_conversion_nullable WHERE if(id % 2, s, NULL) = '3');
SELECT 'Nullable if equality control preserves row', arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM t_bf_conversion_nullable WHERE if(id % 2, s, NULL) = '3';
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nullable_if') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Empty-string if expression declines NULL index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_conversion_nullable WHERE if(id % 2, s, '') IN (''));
SELECT 'Empty-string if expression preserves even rows', arraySort(groupArray(toUInt64(id))) = [toUInt64(0), toUInt64(2), toUInt64(4), toUInt64(6), toUInt64(8), toUInt64(10), toUInt64(12), toUInt64(14)]
FROM t_bf_conversion_nullable WHERE if(id % 2, s, '') IN ('');

SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;
DROP TABLE t_bf_conversion_numeric_wide;
DROP TABLE t_bf_conversion_datetime64;
DROP TABLE t_bf_conversion_nullable;
