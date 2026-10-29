SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 0;

-- A genuine Bool capture must not disable the safe sibling or the existing has matcher.
DROP TABLE IF EXISTS t_bf_bool_capture;
CREATE TABLE t_bf_bool_capture
(
    id UInt64,
    s String,
    flag Bool,
    a Array(UInt64),
    INDEX idx_bool_capture (s, arrayMap(v -> if(flag, v, 0), a)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_bool_capture SELECT number + 1, toString(number), number % 2 = 1, [number] FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_bool_capture WHERE s = '3' SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_bool_capture WHERE s = '3' SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Bool capture sibling preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_bool_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Bool capture sibling prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_bool_capture WHERE s = '3');

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_bool_capture WHERE has(arrayMap(v -> if(flag, v, 0), a), toUInt64(3)) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_bool_capture WHERE has(arrayMap(v -> if(flag, v, 0), a), toUInt64(3)) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'Bool capture has preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_bool_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'Bool capture has prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_bool_capture WHERE has(arrayMap(v -> if(flag, v, 0), a), toUInt64(3)));

-- Every native integer conversion target uses the same settings-independent integer path.
DROP TABLE IF EXISTS t_bf_uint32_capture;
CREATE TABLE t_bf_uint32_capture
(
    id UInt64,
    s String,
    x UInt16,
    INDEX idx_uint32_capture (s, toUInt32(x)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_uint32_capture SELECT number + 1, toString(number), number FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_uint32_capture WHERE s = '3' SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_uint32_capture WHERE s = '3' SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'toUInt32 sibling preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_uint32_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'toUInt32 sibling prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_uint32_capture WHERE s = '3');

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_uint32_capture WHERE toUInt32(x) IN (toUInt32(3)) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_uint32_capture WHERE toUInt32(x) IN (toUInt32(3)) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'toUInt32 IN preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_uint32_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'toUInt32 IN prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_uint32_capture WHERE toUInt32(x) IN (toUInt32(3)));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_uint32_capture WHERE toUInt64(x) IN (toUInt64(3)) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_uint32_capture WHERE toUInt64(x) IN (toUInt64(3)) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'different integer target preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_uint32_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different integer target declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_uint32_capture WHERE toUInt64(x) IN (toUInt64(3)));

-- Multiplication admission retains the ordered operands and exact native types/constants.
DROP TABLE IF EXISTS t_bf_multiply_in;
CREATE TABLE t_bf_multiply_in
(
    id UInt64,
    x UInt64,
    INDEX idx_multiply x * 2 TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_multiply_in SELECT number + 1, number FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_multiply_in WHERE (x * 2) IN (6) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_multiply_in WHERE (x * 2) IN (6) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'integer multiply IN preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_multiply') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'integer multiply IN prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_multiply_in WHERE (x * 2) IN (6));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_multiply_in WHERE (x * 3) IN (9) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_multiply_in WHERE (x * 3) IN (9) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'different multiply constant preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_multiply') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different multiply constant declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_multiply_in WHERE (x * 3) IN (9));

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_multiply_in WHERE (x * toFloat64(2)) IN (toFloat64(6)) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_multiply_in WHERE (x * toFloat64(2)) IN (toFloat64(6)) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'different multiply type preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_multiply') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'different multiply type declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_multiply_in WHERE (x * toFloat64(2)) IN (toFloat64(6)));

-- The inner native lambda is hoisted as a callable capture of the outer lambda.
DROP TABLE IF EXISTS t_bf_nested_capture;
CREATE TABLE t_bf_nested_capture
(
    id UInt64,
    s String,
    a Array(Array(UInt64)),
    INDEX idx_nested_capture (s, arrayMap(v -> arraySum(arrayMap(w -> w + 1, v)), a)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_nested_capture SELECT number + 1, toString(number), [[number]] FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_nested_capture WHERE s = '3' SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_nested_capture WHERE s = '3' SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'nested callable sibling preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nested_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'nested callable sibling prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_nested_capture WHERE s = '3');

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_nested_capture WHERE has(arrayMap(v -> arraySum(arrayMap(w -> w + 1, v)), a), toUInt64(4)) SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_nested_capture WHERE has(arrayMap(v -> arraySum(arrayMap(w -> w + 1, v)), a), toUInt64(4)) SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'nested callable has preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_nested_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'nested callable has prunes 1/16', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_nested_capture WHERE has(arrayMap(v -> arraySum(arrayMap(w -> w + 1, v)), a), toUInt64(4)));

-- A settings-dependent conversion inside a captured callable must still disable the whole index.
DROP TABLE IF EXISTS t_bf_unsafe_nested_capture;
CREATE TABLE t_bf_unsafe_nested_capture
(
    id UInt64,
    s String,
    a Array(Array(String)),
    INDEX idx_unsafe_nested_capture (s, arrayMap(v -> arraySum(arrayMap(w -> toFloat64(w), v)), a)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bf_unsafe_nested_capture SELECT number + 1, toString(number), [[toString(number)]] FROM numbers(16);

WITH
    (SELECT arraySort(groupArray(id)) FROM t_bf_unsafe_nested_capture WHERE s = '3' SETTINGS use_skip_indexes = 0) AS full_scan,
    (SELECT arraySort(groupArray(id)) FROM t_bf_unsafe_nested_capture WHERE s = '3' SETTINGS use_skip_indexes = 1) AS indexed
SELECT 'unsafe nested callable sibling preserves rows', indexed = full_scan AND indexed = [toUInt64(4)];

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_unsafe_nested_capture') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, primary_key_row + 1)) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'unsafe nested callable sibling declines index and reads 16/16', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM t_bf_unsafe_nested_capture WHERE s = '3');

DROP TABLE t_bf_bool_capture;
DROP TABLE t_bf_uint32_capture;
DROP TABLE t_bf_multiply_in;
DROP TABLE t_bf_nested_capture;
DROP TABLE t_bf_unsafe_nested_capture;
