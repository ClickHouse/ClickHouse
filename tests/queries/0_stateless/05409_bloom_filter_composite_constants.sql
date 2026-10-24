DROP TABLE IF EXISTS p2_tuple;
DROP TABLE IF EXISTS p2_tuple_type_mismatch;
DROP TABLE IF EXISTS p2_nested_tuple;
DROP TABLE IF EXISTS p2_array;

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SELECT
    'analyzer_types',
    toTypeName((1, 2)),
    toTypeName((toUInt8(1), toInt8(2))),
    toTypeName(tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))),
    toTypeName([]),
    toTypeName(cityHash64(toUInt64(5), tuple(toUInt8(1), toUInt8(2)))),
    toTypeName(cityHash64(toUInt64(5), tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))))
FORMAT TSVRaw;

CREATE TABLE p2_tuple
(
    id UInt64,
    x UInt64,
    INDEX idx cityHash64(x, (1, 2)) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO p2_tuple SELECT number, number FROM numbers(16);

CREATE TABLE p2_tuple_type_mismatch
(
    id UInt64,
    x UInt64,
    INDEX idx cityHash64(x, tuple(toUInt8(1), toUInt8(2))) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO p2_tuple_type_mismatch SELECT number, number FROM numbers(16);

CREATE TABLE p2_nested_tuple
(
    id UInt64,
    x UInt64,
    INDEX idx cityHash64(x, tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO p2_nested_tuple SELECT number, number FROM numbers(16);

CREATE TABLE p2_array
(
    id UInt64,
    a Array(UInt64),
    INDEX idx arrayConcat(a, []) TYPE bloom_filter(0.000001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO p2_array SELECT number, [number] FROM numbers(16);

SELECT 'tuple_matching_in_plan', toUInt8(countIf(explain LIKE '%Name: idx%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_tuple
      WHERE cityHash64(x, (1, 2)) IN (cityHash64(toUInt64(3), (1, 2)))
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'tuple_matching_in_enabled_rows', arraySort(groupArray(id)) FROM p2_tuple
WHERE cityHash64(x, (1, 2)) IN (cityHash64(toUInt64(3), (1, 2))) FORMAT TSVRaw;
SELECT 'tuple_matching_in_fullscan_rows', arraySort(groupArray(id)) FROM p2_tuple
WHERE cityHash64(x, (1, 2)) IN (cityHash64(toUInt64(3), (1, 2))) SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'tuple_matching_in_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_tuple WHERE cityHash64(x, (1, 2)) IN (cityHash64(toUInt64(3), (1, 2)))) =
    (SELECT arraySort(groupArray(id)) FROM p2_tuple WHERE cityHash64(x, (1, 2)) IN (cityHash64(toUInt64(3), (1, 2))) SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

SELECT 'tuple_equality_control_plan', toUInt8(countIf(explain LIKE '%Name: idx%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_tuple
      WHERE cityHash64(x, (1, 2)) = cityHash64(toUInt64(3), (1, 2))
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'tuple_equality_control_enabled_rows', arraySort(groupArray(id)) FROM p2_tuple
WHERE cityHash64(x, (1, 2)) = cityHash64(toUInt64(3), (1, 2)) FORMAT TSVRaw;
SELECT 'tuple_equality_control_fullscan_rows', arraySort(groupArray(id)) FROM p2_tuple
WHERE cityHash64(x, (1, 2)) = cityHash64(toUInt64(3), (1, 2)) SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'tuple_equality_control_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_tuple WHERE cityHash64(x, (1, 2)) = cityHash64(toUInt64(3), (1, 2))) =
    (SELECT arraySort(groupArray(id)) FROM p2_tuple WHERE cityHash64(x, (1, 2)) = cityHash64(toUInt64(3), (1, 2)) SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

SELECT 'tuple_unequal_value_rejected_plan', toUInt8(countIf(explain LIKE '%Name: idx%') = 0 AND countIf(explain LIKE '%Granules: 16/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_tuple
      WHERE cityHash64(x, (1, 3)) IN (cityHash64(toUInt64(5), (1, 3)))
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'tuple_unequal_value_enabled_rows', arraySort(groupArray(id)) FROM p2_tuple
WHERE cityHash64(x, (1, 3)) IN (cityHash64(toUInt64(5), (1, 3))) FORMAT TSVRaw;
SELECT 'tuple_unequal_value_fullscan_rows', arraySort(groupArray(id)) FROM p2_tuple
WHERE cityHash64(x, (1, 3)) IN (cityHash64(toUInt64(5), (1, 3))) SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'tuple_unequal_value_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_tuple WHERE cityHash64(x, (1, 3)) IN (cityHash64(toUInt64(5), (1, 3)))) =
    (SELECT arraySort(groupArray(id)) FROM p2_tuple WHERE cityHash64(x, (1, 3)) IN (cityHash64(toUInt64(5), (1, 3))) SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

SELECT 'tuple_child_type_mismatch_rejected_plan', toUInt8(countIf(explain LIKE '%Name: idx%') = 0 AND countIf(explain LIKE '%Granules: 16/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_tuple_type_mismatch
      WHERE cityHash64(x, tuple(toInt8(1), toInt8(2))) IN (cityHash64(toUInt64(5), tuple(toInt8(1), toInt8(2))))
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'tuple_child_type_mismatch_enabled_rows', arraySort(groupArray(id)) FROM p2_tuple_type_mismatch
WHERE cityHash64(x, tuple(toInt8(1), toInt8(2))) IN (cityHash64(toUInt64(5), tuple(toInt8(1), toInt8(2)))) FORMAT TSVRaw;
SELECT 'tuple_child_type_mismatch_fullscan_rows', arraySort(groupArray(id)) FROM p2_tuple_type_mismatch
WHERE cityHash64(x, tuple(toInt8(1), toInt8(2))) IN (cityHash64(toUInt64(5), tuple(toInt8(1), toInt8(2)))) SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'tuple_child_type_mismatch_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_tuple_type_mismatch WHERE cityHash64(x, tuple(toInt8(1), toInt8(2))) IN (cityHash64(toUInt64(5), tuple(toInt8(1), toInt8(2))))) =
    (SELECT arraySort(groupArray(id)) FROM p2_tuple_type_mismatch WHERE cityHash64(x, tuple(toInt8(1), toInt8(2))) IN (cityHash64(toUInt64(5), tuple(toInt8(1), toInt8(2)))) SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

SELECT 'nested_tuple_unsupported_rejected_plan', toUInt8(countIf(explain LIKE '%Name: idx%') = 0 AND countIf(explain LIKE '%Granules: 16/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_nested_tuple
      WHERE cityHash64(x, tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))) IN
            (cityHash64(toUInt64(5), tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))))
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'nested_tuple_unsupported_enabled_rows', arraySort(groupArray(id)) FROM p2_nested_tuple
WHERE cityHash64(x, tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))) IN
      (cityHash64(toUInt64(5), tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3)))) FORMAT TSVRaw;
SELECT 'nested_tuple_unsupported_fullscan_rows', arraySort(groupArray(id)) FROM p2_nested_tuple
WHERE cityHash64(x, tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))) IN
      (cityHash64(toUInt64(5), tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3)))) SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'nested_tuple_unsupported_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_nested_tuple WHERE cityHash64(x, tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))) IN (cityHash64(toUInt64(5), tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))))) =
    (SELECT arraySort(groupArray(id)) FROM p2_nested_tuple WHERE cityHash64(x, tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3))) IN (cityHash64(toUInt64(5), tuple(tuple(toUInt8(1), toUInt8(2)), toUInt8(3)))) SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

SELECT 'array_nothing_direct_in_plan', toUInt8(countIf(explain LIKE '%Name: idx%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_array WHERE arrayConcat(a, []) IN ([3])
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'array_nothing_direct_in_enabled_rows', arraySort(groupArray(id)) FROM p2_array WHERE arrayConcat(a, []) IN ([3]) FORMAT TSVRaw;
SELECT 'array_nothing_direct_in_fullscan_rows', arraySort(groupArray(id)) FROM p2_array WHERE arrayConcat(a, []) IN ([3]) SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'array_nothing_direct_in_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_array WHERE arrayConcat(a, []) IN ([3])) =
    (SELECT arraySort(groupArray(id)) FROM p2_array WHERE arrayConcat(a, []) IN ([3]) SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

SELECT 'array_nothing_array_join_in_plan', toUInt8(countIf(explain LIKE '%Name: idx%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) IN (3)
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'array_nothing_array_join_in_enabled_rows', arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) IN (3) FORMAT TSVRaw;
SELECT 'array_nothing_array_join_in_fullscan_rows', arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) IN (3) SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'array_nothing_array_join_in_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) IN (3)) =
    (SELECT arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) IN (3) SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

SELECT 'array_nothing_array_join_equality_control_plan', toUInt8(countIf(explain LIKE '%Name: idx%') > 0 AND countIf(explain LIKE '%Granules: 1/16%') > 0)
FROM (EXPLAIN indexes = 1 SELECT id FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) = 3
      SETTINGS use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0) FORMAT TSVRaw;
SELECT 'array_nothing_array_join_equality_control_enabled_rows', arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) = 3 FORMAT TSVRaw;
SELECT 'array_nothing_array_join_equality_control_fullscan_rows', arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) = 3 SETTINGS use_skip_indexes = 0 FORMAT TSVRaw;
SELECT 'array_nothing_array_join_equality_control_rows_equal', toUInt8(
    (SELECT arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) = 3) =
    (SELECT arraySort(groupArray(id)) FROM p2_array WHERE arrayJoin(arrayConcat(a, [])) = 3 SETTINGS use_skip_indexes = 0)) FORMAT TSVRaw;

DROP TABLE IF EXISTS p2_tuple;
DROP TABLE IF EXISTS p2_tuple_type_mismatch;
DROP TABLE IF EXISTS p2_nested_tuple;
DROP TABLE IF EXISTS p2_array;
