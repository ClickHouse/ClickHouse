SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_bf_to_uint64_context;
DROP TABLE IF EXISTS t_bf_minus_direct_in;
DROP TABLE IF EXISTS t_bf_array_slice_string;

-- A context-independent integer conversion in one component must not disable pruning
-- by the safe sibling component of the same composite index.
CREATE TABLE t_bf_to_uint64_context
(
    id UInt64,
    x UInt32,
    s String,
    INDEX idx_s_and_to_uint64 (s, toUInt64(x)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_to_uint64_context
SELECT number + 1, number, toString(number)
FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_to_uint64_context WHERE toUInt64(x) IN (toUInt64(3)) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_to_uint64_context WHERE toUInt64(x) IN (toUInt64(3)) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'toUInt64(UInt32) direct IN preserves typed rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_s_and_to_uint64') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'toUInt64(UInt32) direct IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_to_uint64_context WHERE toUInt64(x) IN (toUInt64(3)));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_to_uint64_context WHERE s = '3' SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_to_uint64_context WHERE s = '3' SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'toUInt64(UInt32) context keeps safe sibling typed rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_s_and_to_uint64') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'toUInt64(UInt32) context keeps safe sibling pruning at 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_to_uint64_context WHERE s = '3');

-- The integer subtraction signature is admitted only for the exact same
-- native function and ordered operands as the index expression.
CREATE TABLE t_bf_minus_direct_in
(
    id UInt64,
    x Int64,
    INDEX idx_minus x - 1 TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_minus_direct_in
SELECT number + 1, toInt64(number)
FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_minus_direct_in WHERE (x - 1) IN (2) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_minus_direct_in WHERE (x - 1) IN (2) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'integer minus direct IN preserves typed rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_minus') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'integer minus direct IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_minus_direct_in WHERE (x - 1) IN (2));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_minus_direct_in WHERE (1 - x) IN (-2) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_minus_direct_in WHERE (1 - x) IN (-2) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'reversed integer minus preserves typed rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_minus') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'reversed integer minus declines idx_minus and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_minus_direct_in WHERE (1 - x) IN (-2));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_minus_direct_in WHERE (x - 2) IN (1) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_minus_direct_in WHERE (x - 2) IN (1) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'different integer minus literal preserves typed rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_minus') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different integer minus literal declines idx_minus and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_minus_direct_in WHERE (x - 2) IN (1));

-- Array(String) is supported for the two-argument arraySlice case, but a
-- different slice offset must not reuse the index expression.
CREATE TABLE t_bf_array_slice_string
(
    id UInt64,
    a Array(String),
    INDEX idx_array_slice_one arraySlice(a, 1) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_bf_array_slice_string
SELECT number + 1,
       [if(number = 2, 'target', concat('value-', toString(number))), concat('tail-', toString(number))]
FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_array_slice_string WHERE arraySlice(a, 1) IN (['target', 'tail-2']) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_array_slice_string WHERE arraySlice(a, 1) IN (['target', 'tail-2']) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Array(String) arraySlice direct IN preserves typed rows', indexed = full_scan AND indexed = [toUInt64(3)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_slice_one') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Array(String) arraySlice direct IN uses named index and prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_array_slice_string WHERE arraySlice(a, 1) IN (['target', 'tail-2']));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_array_slice_string WHERE arraySlice(a, 2) IN (['tail-2']) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_array_slice_string WHERE arraySlice(a, 2) IN (['tail-2']) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'different Array(String) arraySlice offset preserves typed rows', indexed = full_scan AND indexed = [toUInt64(3)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_slice_one') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different Array(String) arraySlice offset declines named index, reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_array_slice_string WHERE arraySlice(a, 2) IN (['tail-2']));

DROP TABLE IF EXISTS t_bf_to_uint64_context;
DROP TABLE IF EXISTS t_bf_minus_direct_in;
DROP TABLE IF EXISTS t_bf_array_slice_string;
