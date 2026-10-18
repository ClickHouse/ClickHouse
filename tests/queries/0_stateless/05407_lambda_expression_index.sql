SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS lambda_expression_index;
CREATE TABLE lambda_expression_index
(
    id UInt64,
    arr Array(UInt64),
    offset_value UInt64,
    other_offset UInt64,
    INDEX idx_plain arrayMap(v -> v + 1, arr) TYPE bloom_filter(0.000001) GRANULARITY 1,
    INDEX idx_capture arrayMap(v -> v + offset_value, arr) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO lambda_expression_index
SELECT number, [number], toUInt64(1), toUInt64(2)
FROM numbers(16);

SELECT 'plain direct IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + 1, arr) IN ([toUInt64(4)]);
SELECT 'plain direct IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + 1, arr) IN ([toUInt64(4)])
SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_plain') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'plain direct IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index WHERE arrayMap(v -> v + 1, arr) IN ([toUInt64(4)]));

SELECT 'plain arrayJoin IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + 1, arr)) IN (4)
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
    query_plan_fuse_filter_into_array_join = 1;
SELECT 'plain arrayJoin IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + 1, arr)) IN (4)
SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1,
    query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_plain') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'plain arrayJoin IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index
    WHERE arrayJoin(arrayMap(v -> v + 1, arr)) IN (4)
    SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
        query_plan_fuse_filter_into_array_join = 1);

SELECT 'different body direct IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + 2, arr) IN ([toUInt64(4)]);
SELECT 'different body direct IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + 2, arr) IN ([toUInt64(4)])
SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_plain') > 0, plan) AS plain_name_row,
    arrayFirstIndex(x -> position(x, 'Name: idx_capture') > 0, plan) AS capture_name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different body direct IN declines indexes and reads 16/16',
       plain_name_row = 0 AND capture_name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index WHERE arrayMap(v -> v + 2, arr) IN ([toUInt64(4)]));

SELECT 'different body arrayJoin IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + 2, arr)) IN (4)
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
    query_plan_fuse_filter_into_array_join = 1;
SELECT 'different body arrayJoin IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + 2, arr)) IN (4)
SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1,
    query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_plain') > 0, plan) AS plain_name_row,
    arrayFirstIndex(x -> position(x, 'Name: idx_capture') > 0, plan) AS capture_name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different body arrayJoin IN declines indexes and reads 16/16',
       plain_name_row = 0 AND capture_name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index
    WHERE arrayJoin(arrayMap(v -> v + 2, arr)) IN (4)
    SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
        query_plan_fuse_filter_into_array_join = 1);

SELECT 'captured direct IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + offset_value, arr) IN ([toUInt64(4)]);
SELECT 'captured direct IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + offset_value, arr) IN ([toUInt64(4)])
SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_capture') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'captured direct IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index
    WHERE arrayMap(v -> v + offset_value, arr) IN ([toUInt64(4)]));

SELECT 'captured arrayJoin IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + offset_value, arr)) IN (4)
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
    query_plan_fuse_filter_into_array_join = 1;
SELECT 'captured arrayJoin IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(3)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + offset_value, arr)) IN (4)
SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1,
    query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_capture') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'captured arrayJoin IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index
    WHERE arrayJoin(arrayMap(v -> v + offset_value, arr)) IN (4)
    SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
        query_plan_fuse_filter_into_array_join = 1);

SELECT 'different capture direct IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + other_offset, arr) IN ([toUInt64(4)]);
SELECT 'different capture direct IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayMap(v -> v + other_offset, arr) IN ([toUInt64(4)])
SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_plain') > 0, plan) AS plain_name_row,
    arrayFirstIndex(x -> position(x, 'Name: idx_capture') > 0, plan) AS capture_name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different capture direct IN declines indexes and reads 16/16',
       plain_name_row = 0 AND capture_name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index
    WHERE arrayMap(v -> v + other_offset, arr) IN ([toUInt64(4)]));

SELECT 'different capture arrayJoin IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + other_offset, arr)) IN (4)
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
    query_plan_fuse_filter_into_array_join = 1;
SELECT 'different capture arrayJoin IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_expression_index
WHERE arrayJoin(arrayMap(v -> v + other_offset, arr)) IN (4)
SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1,
    query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_plain') > 0, plan) AS plain_name_row,
    arrayFirstIndex(x -> position(x, 'Name: idx_capture') > 0, plan) AS capture_name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different capture arrayJoin IN declines indexes and reads 16/16',
       plain_name_row = 0 AND capture_name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_expression_index
    WHERE arrayJoin(arrayMap(v -> v + other_offset, arr)) IN (4)
    SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
        query_plan_fuse_filter_into_array_join = 1);

DROP TABLE lambda_expression_index;

DROP TABLE IF EXISTS lambda_capture_guard;
CREATE TABLE lambda_capture_guard
(
    id UInt64,
    arr Array(UInt64),
    varying_offset UInt64,
    INDEX idx_u64_const arrayMap(v -> v + toUInt64(1), arr) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO lambda_capture_guard
SELECT number, [number], toUInt64(if(number % 2 = 0, 2, 1))
FROM numbers(16);

SELECT 'UInt64 row capture direct IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2), toUInt64(3)]
FROM lambda_capture_guard
WHERE arrayMap(v -> v + varying_offset, arr) IN ([toUInt64(4)]);
SELECT 'UInt64 row capture direct IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2), toUInt64(3)]
FROM lambda_capture_guard
WHERE arrayMap(v -> v + varying_offset, arr) IN ([toUInt64(4)])
SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_u64_const') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt64 row capture direct IN declines index and reads 16/16',
       name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_capture_guard
    WHERE arrayMap(v -> v + varying_offset, arr) IN ([toUInt64(4)]));

SELECT 'UInt64 row capture arrayJoin IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2), toUInt64(3)]
FROM lambda_capture_guard
WHERE arrayJoin(arrayMap(v -> v + varying_offset, arr)) IN (toUInt64(4))
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
    query_plan_fuse_filter_into_array_join = 1;
SELECT 'UInt64 row capture arrayJoin IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2), toUInt64(3)]
FROM lambda_capture_guard
WHERE arrayJoin(arrayMap(v -> v + varying_offset, arr)) IN (toUInt64(4))
SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1,
    query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_u64_const') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'UInt64 row capture arrayJoin IN declines index and reads 16/16',
       name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_capture_guard
    WHERE arrayJoin(arrayMap(v -> v + varying_offset, arr)) IN (toUInt64(4))
    SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
        query_plan_fuse_filter_into_array_join = 1);

SELECT 'changed UInt64 constant direct IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_capture_guard
WHERE arrayMap(v -> v + toUInt64(2), arr) IN ([toUInt64(4)]);
SELECT 'changed UInt64 constant direct IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_capture_guard
WHERE arrayMap(v -> v + toUInt64(2), arr) IN ([toUInt64(4)])
SETTINGS use_skip_indexes = 0;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_u64_const') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'changed UInt64 constant direct IN declines index and reads 16/16',
       name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_capture_guard
    WHERE arrayMap(v -> v + toUInt64(2), arr) IN ([toUInt64(4)]));

SELECT 'changed UInt64 constant arrayJoin IN indexed rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_capture_guard
WHERE arrayJoin(arrayMap(v -> v + toUInt64(2), arr)) IN (toUInt64(4))
SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
    query_plan_fuse_filter_into_array_join = 1;
SELECT 'changed UInt64 constant arrayJoin IN full scan rows',
       arraySort(groupArray(toUInt64(id))) = [toUInt64(2)]
FROM lambda_capture_guard
WHERE arrayJoin(arrayMap(v -> v + toUInt64(2), arr)) IN (toUInt64(4))
SETTINGS use_skip_indexes = 0, query_plan_lower_array_join_function = 1,
    query_plan_enable_optimizations = 1, query_plan_fuse_filter_into_array_join = 1;
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_u64_const') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'changed UInt64 constant arrayJoin IN declines index and reads 16/16',
       name_row = 0 AND primary_key_row > 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM lambda_capture_guard
    WHERE arrayJoin(arrayMap(v -> v + toUInt64(2), arr)) IN (toUInt64(4))
    SETTINGS query_plan_lower_array_join_function = 1, query_plan_enable_optimizations = 1,
        query_plan_fuse_filter_into_array_join = 1);

DROP TABLE lambda_capture_guard;
