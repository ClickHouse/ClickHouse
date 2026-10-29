SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET optimize_functions_to_subcolumns = 0;
SET input_format_values_interpret_expressions = 1;

DROP TABLE IF EXISTS p1;
DROP TABLE IF EXISTS p1_safe;
DROP TABLE IF EXISTS p1_abs;
DROP TABLE IF EXISTS p1_negate;
DROP TABLE IF EXISTS p1_sign;
DROP TABLE IF EXISTS p1_bit_not;

SET formatdatetime_parsedatetime_m_is_month_name = 1;
CREATE TABLE p1
(
    id UInt64,
    ts DateTime('UTC'),
    INDEX idx_formatter [formatDateTime(ts, '%M')] TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO p1 SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') FROM numbers(16);
SET formatdatetime_parsedatetime_m_is_month_name = 0;
DETACH TABLE p1;
ATTACH TABLE p1;
SELECT 'formatter query setting is 0', value = '0' FROM system.settings WHERE name = 'formatdatetime_parsedatetime_m_is_month_name';

WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_formatter') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'formatter mismatch declines pruning after reattach', name_row = 0 AND selected = 16 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM p1
    WHERE arrayJoin(CAST(CAST([formatDateTime(ts, '%M')] AS Dynamic) AS Array(String))) IN ('00')
    SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0, use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0);
SELECT 'formatter indexed results preserve rows', arraySort(groupArray(id)) = arrayMap(x -> toUInt64(x), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
FROM p1 WHERE arrayJoin(CAST(CAST([formatDateTime(ts, '%M')] AS Dynamic) AS Array(String))) IN ('00')
SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT 'formatter fullscan preserves rows', arraySort(groupArray(id)) = arrayMap(x -> toUInt64(x), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
FROM p1 WHERE arrayJoin(CAST(CAST([formatDateTime(ts, '%M')] AS Dynamic) AS Array(String))) IN ('00')
SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0, use_skip_indexes = 0;

CREATE TABLE p1_safe
(
    id UInt64,
    a Array(String),
    INDEX idx_array_distinct arrayDistinct(a) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO p1_safe SELECT number, [if(number = 3, 'target', concat('value-', toString(number)))] FROM numbers(16);
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_array_distinct') > 0, plan) AS name_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'arrayDistinct Dynamic arrayJoin retains 1 of 16 granules', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM p1_safe
    WHERE arrayJoin(CAST(CAST(arrayDistinct(a) AS Dynamic) AS Array(String))) IN ('target')
    SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0);
SELECT 'arrayDistinct indexed result is nonempty target row', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_safe WHERE arrayJoin(CAST(CAST(arrayDistinct(a) AS Dynamic) AS Array(String))) IN ('target');
SELECT 'arrayDistinct fullscan agrees', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_safe WHERE arrayJoin(CAST(CAST(arrayDistinct(a) AS Dynamic) AS Array(String))) IN ('target') SETTINGS use_skip_indexes = 0;

CREATE TABLE p1_abs
(
    id UInt64,
    x Int64,
    INDEX idx_abs abs(x) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO p1_abs SELECT number, toInt64(number) FROM numbers(16);
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_abs') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'abs IN retains 1 of 16 granules', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM p1_abs WHERE abs(x) IN (toInt64(3))
    SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0);
SELECT 'abs indexed rows are nonempty target row', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_abs WHERE abs(x) IN (toInt64(3));
SELECT 'abs fullscan rows agree', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_abs WHERE abs(x) IN (toInt64(3)) SETTINGS use_skip_indexes = 0;

CREATE TABLE p1_negate
(
    id UInt64,
    x Int64,
    INDEX idx_negate negate(x) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO p1_negate SELECT number, toInt64(number) FROM numbers(16);
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_negate') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'negate IN retains 1 of 16 granules', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM p1_negate WHERE negate(x) IN (toInt64(-3))
    SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0);
SELECT 'negate indexed rows are nonempty target row', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_negate WHERE negate(x) IN (toInt64(-3));
SELECT 'negate fullscan rows agree', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_negate WHERE negate(x) IN (toInt64(-3)) SETTINGS use_skip_indexes = 0;

CREATE TABLE p1_sign
(
    id UInt64,
    x Int64,
    INDEX idx_sign sign(x) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO p1_sign SELECT number, toInt64(number) FROM numbers(16);
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_sign') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'sign IN retains 1 of 16 granules', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM p1_sign WHERE sign(x) IN (toInt8(0))
    SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0);
SELECT 'sign indexed rows are nonempty target row', arraySort(groupArray(id)) = [toUInt64(0)]
FROM p1_sign WHERE sign(x) IN (toInt8(0));
SELECT 'sign fullscan rows agree', arraySort(groupArray(id)) = [toUInt64(0)]
FROM p1_sign WHERE sign(x) IN (toInt8(0)) SETTINGS use_skip_indexes = 0;

CREATE TABLE p1_bit_not
(
    id UInt64,
    x Int64,
    INDEX idx_bit_not bitNot(x) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO p1_bit_not SELECT number, toInt64(number) FROM numbers(16);
WITH
    groupArray(explain) AS plan,
    arrayFirstIndex(x -> position(x, 'Name: idx_bit_not') > 0, plan) AS name_row,
    arrayFirstIndex(x -> position(x, 'PrimaryKey') > 0, plan) AS primary_key_row,
    arrayFirst(x -> position(x, 'Granules:') > 0, arraySlice(plan, if(name_row > 0, name_row + 1, primary_key_row + 1))) AS granules_row,
    toUInt64OrZero(extract(granules_row, 'Granules: ([0-9]+)/')) AS selected,
    toUInt64OrZero(extract(granules_row, 'Granules: [0-9]+/([0-9]+)')) AS total
SELECT 'bitNot IN retains 1 of 16 granules', name_row > 0 AND selected = 1 AND total = 16
FROM (EXPLAIN indexes = 1 SELECT id FROM p1_bit_not WHERE bitNot(x) IN (toInt64(-4))
    SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0);
SELECT 'bitNot indexed rows are nonempty target row', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_bit_not WHERE bitNot(x) IN (toInt64(-4));
SELECT 'bitNot fullscan rows agree', arraySort(groupArray(id)) = [toUInt64(3)]
FROM p1_bit_not WHERE bitNot(x) IN (toInt64(-4)) SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS p1;
DROP TABLE IF EXISTS p1_safe;
DROP TABLE IF EXISTS p1_abs;
DROP TABLE IF EXISTS p1_negate;
DROP TABLE IF EXISTS p1_sign;
DROP TABLE IF EXISTS p1_bit_not;
