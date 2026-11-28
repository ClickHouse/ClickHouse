-- Bucketed Map in Compact parts written with write_marks_for_substreams_in_compact_parts = 0 must keep the inserted key order.

DROP TABLE IF EXISTS t_map_buckets_order;
DROP TABLE IF EXISTS t_map_buckets_order_merge;
DROP TABLE IF EXISTS t_map_buckets_order_nested;

CREATE TABLE t_map_buckets_order (id UInt32, m Map(String, UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0,
    write_marks_for_substreams_in_compact_parts = 0, min_bytes_for_wide_part = '200G', min_rows_for_wide_part = 1000000000,
    serialization_info_version = 'with_types';

INSERT INTO t_map_buckets_order SELECT 1, mapFromArrays(arrayMap(i -> concat('key', toString(i)), range(100)), range(100));

SELECT 'A', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_map_buckets_order' AND active;
SELECT 'A', arraySlice(mapKeys(m), 1, 5) FROM t_map_buckets_order SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'A', arraySlice(mapKeys(m), 1, 5) FROM t_map_buckets_order SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'A', arraySlice(m.keys, 1, 5), arraySlice(m.values, 1, 5) FROM t_map_buckets_order;
SELECT 'A', m = mapFromArrays(arrayMap(i -> concat('key', toString(i)), range(100)), range(100)) FROM t_map_buckets_order;

OPTIMIZE TABLE t_map_buckets_order FINAL;

SELECT 'B', part_type, level > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_map_buckets_order' AND active;
SELECT 'B', arraySlice(mapKeys(m), 1, 5) FROM t_map_buckets_order;
SELECT 'B', m = mapFromArrays(arrayMap(i -> concat('key', toString(i)), range(100)), range(100)) FROM t_map_buckets_order;

CREATE TABLE t_map_buckets_order_merge (id UInt32, m Map(String, UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 4, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0,
    write_marks_for_substreams_in_compact_parts = 0, min_bytes_for_wide_part = '200G', min_rows_for_wide_part = 1000000000,
    serialization_info_version = 'with_types';

INSERT INTO t_map_buckets_order_merge SELECT 1, mapFromArrays(arrayMap(i -> concat('key', toString(i)), range(100)), range(100));
INSERT INTO t_map_buckets_order_merge SELECT 2, mapFromArrays(arrayMap(i -> concat('key', toString(i)), range(100)), range(100));
OPTIMIZE TABLE t_map_buckets_order_merge FINAL;

SELECT 'C', part_type, level > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_map_buckets_order_merge' AND active;
SELECT 'C', id, arraySlice(mapKeys(m), 1, 3) FROM t_map_buckets_order_merge ORDER BY id;

CREATE TABLE t_map_buckets_order_nested (id UInt32, tup Tuple(a Map(String, UInt32))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0,
    write_marks_for_substreams_in_compact_parts = 0, min_bytes_for_wide_part = '200G', min_rows_for_wide_part = 1000000000,
    serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1;

INSERT INTO t_map_buckets_order_nested SELECT 1, tuple(mapFromArrays(arrayMap(i -> concat('key', toString(i)), range(100)), range(100)));

SELECT 'D', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_map_buckets_order_nested' AND active;
SELECT 'D', arraySlice(mapKeys(tup.a), 1, 5) FROM t_map_buckets_order_nested;
SELECT 'D', arraySlice(tup.a.keys, 1, 5) FROM t_map_buckets_order_nested;

DROP TABLE t_map_buckets_order;
DROP TABLE t_map_buckets_order_merge;
DROP TABLE t_map_buckets_order_nested;
