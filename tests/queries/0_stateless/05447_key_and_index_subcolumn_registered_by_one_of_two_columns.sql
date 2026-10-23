-- `j.m.keys` is claimed by two columns: the keys of Map `j.m` and the path `m.keys` of JSON `j`. Queries read it from
-- the column it is registered under, so skip indexes, sorting and partition keys must be computed from that column too.

SET materialize_skip_indexes_on_insert = 1, use_query_condition_cache = 0;

SELECT '-- skip index, horizontal and vertical merge';
CREATE TABLE t_horizontal (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(LowCardinality(String))), INDEX i j.m.keys TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;
CREATE TABLE t_vertical (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(LowCardinality(String))), INDEX i j.m.keys TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
INSERT INTO t_horizontal SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
INSERT INTO t_horizontal SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
INSERT INTO t_vertical SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
INSERT INTO t_vertical SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
OPTIMIZE TABLE t_horizontal FINAL;
OPTIMIZE TABLE t_vertical FINAL;
SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm FROM system.part_log
WHERE table_uuid IN (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name IN ('t_horizontal', 't_vertical'))
    AND event_type = 'MergeParts'
ORDER BY table;
SELECT count() FROM t_horizontal WHERE has(j.m.keys, 'a');
SELECT count() FROM t_horizontal WHERE has(j.m.keys, 'a') SETTINGS ignore_data_skipping_indices = 'i';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_horizontal WHERE has(j.m.keys, 'nokey') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Granules: %';
SELECT count() FROM t_vertical WHERE has(j.m.keys, 'a');
SELECT count() FROM t_vertical WHERE has(j.m.keys, 'a') SETTINGS ignore_data_skipping_indices = 'i';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_vertical WHERE has(j.m.keys, 'nokey') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Granules: %';

SELECT '-- untyped JSON declared first, insert and merge';
CREATE TABLE t_json_first (id UInt64, j JSON, `j.m` Map(String, String), INDEX i j.m.keys TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_json_first SELECT number, '{"m":{"keys":["zzz"]}}', map('a', toString(number)) FROM numbers(100);
INSERT INTO t_json_first SELECT number, '{"m":{"keys":["zzz"]}}', map('a', toString(number)) FROM numbers(100);
SELECT count() FROM t_json_first WHERE has(j.m.keys, 'a');
SELECT count() FROM t_json_first WHERE has(j.m.keys, 'a') SETTINGS ignore_data_skipping_indices = 'i';
OPTIMIZE TABLE t_json_first FINAL;
SELECT count() FROM t_json_first WHERE has(j.m.keys, 'a');
SELECT count() FROM t_json_first WHERE has(j.m.keys, 'a') SETTINGS ignore_data_skipping_indices = 'i';

SELECT '-- sorting key';
CREATE TABLE t_sorting_key (id UInt64, j JSON, `j.m` Map(String, String))
ENGINE = MergeTree ORDER BY (j.m.keys, id)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_sorting_key SELECT number, '{"m":{"keys":["zzz"]}}', map('a', toString(number)) FROM numbers(100);
INSERT INTO t_sorting_key SELECT number, '{"m":{"keys":["zzz"]}}', map('a', toString(number)) FROM numbers(100);
SELECT count(), countIf(j.m.keys = ['a']) FROM t_sorting_key;
SELECT count() FROM t_sorting_key WHERE j.m.keys = ['a'];
OPTIMIZE TABLE t_sorting_key FINAL;
SELECT count() FROM t_sorting_key WHERE j.m.keys = ['a'];

SELECT '-- partition key';
CREATE TABLE t_partition_key (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)))
ENGINE = MergeTree PARTITION BY j.m.keys ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_partition_key SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
SELECT count(), countIf(j.m.keys = ['a']) FROM t_partition_key;
SELECT count() FROM t_partition_key WHERE j.m.keys = ['a'];
SELECT DISTINCT _partition_value FROM t_partition_key;

SELECT '-- mutation rewriting a compact part';
CREATE TABLE t_mutation (id UInt64, j JSON, `j.m` Map(String, String), INDEX i j.m.keys TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10M', min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;
INSERT INTO t_mutation SELECT number, '{"m":{"keys":["zzz"]}}', map('a', toString(number)) FROM numbers(100);
ALTER TABLE t_mutation UPDATE `j.m` = map('b', toString(id)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM t_mutation WHERE has(j.m.keys, 'b');
SELECT count() FROM t_mutation WHERE has(j.m.keys, 'b') SETTINGS ignore_data_skipping_indices = 'i';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_mutation WHERE has(j.m.keys, 'nokey') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Granules: %';

SELECT '-- part written before the column was added';
CREATE TABLE t_old_part (id UInt64, j JSON, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = '10G';
INSERT INTO t_old_part SELECT number, '{"m":{"keys":["zzz"]}}', number FROM numbers(100);
ALTER TABLE t_old_part ADD COLUMN `j.m` Map(String, String);
INSERT INTO t_old_part SELECT number + 100, '{"m":{"keys":["zzz"]}}', number, map('a', toString(number)) FROM numbers(100);
ALTER TABLE t_old_part ADD INDEX i j.m.keys TYPE bloom_filter GRANULARITY 1;
ALTER TABLE t_old_part MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_old_part WHERE has(j.m.keys, 'nokey') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Granules: %';
ALTER TABLE t_old_part UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM t_old_part WHERE has(j.m.keys, 'a');
SELECT count() FROM t_old_part WHERE has(j.m.keys, 'a') SETTINGS ignore_data_skipping_indices = 'i';
SELECT replaceRegexpOne(explain, '^[│└├─ ]+', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_old_part WHERE has(j.m.keys, 'nokey') SETTINGS use_skip_indexes_on_data_read = 0) WHERE explain ILIKE '%Granules: %';

SELECT '-- TTL GROUP BY re-sorting by the sorting key';
CREATE TABLE t_ttl_group_by (`j.m` Map(String, String), j JSON(`m.keys` Array(String)), ts DateTime('UTC'), v UInt32)
ENGINE = MergeTree ORDER BY j.m.keys
TTL ts + toIntervalDay(1) GROUP BY j.m.keys SET `j.m` = map(toString(1000 - max(v)), ''), ts = max(ts) + INTERVAL 100 YEAR
SETTINGS index_granularity = 1;
INSERT INTO t_ttl_group_by SELECT map(toString(number), ''), '{"m":{"keys":["zzz"]}}', '2000-06-09 10:00:00', number FROM numbers(1, 20);
OPTIMIZE TABLE t_ttl_group_by FINAL;
SELECT count(), countIf(j.m.keys = ['999']) FROM t_ttl_group_by;
SELECT count() FROM t_ttl_group_by WHERE j.m.keys = ['999'];
OPTIMIZE TABLE t_ttl_group_by FINAL;
SELECT count() FROM t_ttl_group_by;

SELECT '-- partition key min-max index after a merge or mutation that removes rows';
CREATE TABLE t_replacing (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)))
ENGINE = ReplacingMergeTree PARTITION BY j.m.keys ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;
INSERT INTO t_replacing SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
INSERT INTO t_replacing SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
OPTIMIZE TABLE t_replacing FINAL;
SELECT count(), countIf(j.m.keys = ['a']) FROM t_replacing;
SELECT count() FROM t_replacing WHERE j.m.keys = ['a'];
CREATE TABLE t_delete (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)))
ENGINE = MergeTree PARTITION BY j.m.keys ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_delete SELECT number, map('a', toString(number)), '{"m":{"keys":["zzz"]}}' FROM numbers(100);
ALTER TABLE t_delete DELETE WHERE id < 50 SETTINGS mutations_sync = 2;
SELECT count(), countIf(j.m.keys = ['a']) FROM t_delete;
SELECT count() FROM t_delete WHERE j.m.keys = ['a'];
CREATE TABLE t_tuple_vertical (id UInt64, t Tuple(a UInt32, b String), v String)
ENGINE = ReplacingMergeTree PARTITION BY t.a ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
INSERT INTO t_tuple_vertical SELECT number, (1, 'x'), 'v' FROM numbers(100);
INSERT INTO t_tuple_vertical SELECT number, (1, 'x'), 'v' FROM numbers(100);
OPTIMIZE TABLE t_tuple_vertical FINAL;
SELECT count() FROM t_tuple_vertical WHERE t.a = 1;
CREATE TABLE t_json_path_vertical (id UInt64, j JSON, v String)
ENGINE = ReplacingMergeTree PARTITION BY j.p.:Int64 ORDER BY id
SETTINGS allow_nullable_key = 1, index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
INSERT INTO t_json_path_vertical SELECT number, '{"p":1}', 'v' FROM numbers(100);
INSERT INTO t_json_path_vertical SELECT number, '{"p":1}', 'v' FROM numbers(100);
OPTIMIZE TABLE t_json_path_vertical FINAL;
SELECT count() FROM t_json_path_vertical WHERE j.p.:Int64 = 1;
