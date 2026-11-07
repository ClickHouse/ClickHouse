-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag no-parallel-replicas: the reads must run in one local stream for the threshold trajectory to be deterministic

-- A TopK read (`ORDER BY k LIMIT n`, `GROUP BY k LIMIT n`) records in the query condition cache the granules whose rows
-- all sort strictly beyond the final threshold, not only the granules `__topKFilter` emptied against the running one.

SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering_for_variable_length_types = 0;
SET use_top_k_dynamic_filtering_for_single_string_sort_column = 1;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET optimize_trivial_group_by_limit_query = 0;
SET max_rows_to_group_by = 0;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
-- One thread and one granule per block, so the threshold trajectory is deterministic.
SET max_threads = 1;
SET max_block_size = 64;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.;
SET merge_tree_min_rows_for_seek = 0;
SET merge_tree_min_bytes_for_seek = 0;
SET max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS tab_int;
DROP TABLE IF EXISTS tab_dt64;
DROP TABLE IF EXISTS tab_str;
DROP TABLE IF EXISTS tab_ties;
DROP TABLE IF EXISTS tab_group;
DROP TABLE IF EXISTS tab_default;
DROP TABLE IF EXISTS tab_alter;

SYSTEM CLEAR QUERY CONDITION CACHE;

-- `k` decreases along the part: each granule has rows below the threshold the granules before it published, so
-- `__topKFilter` empties none of the 100 granules. Only the last one holds rows of the result.
CREATE TABLE tab_int (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0,
    auto_statistics_types = '';
SYSTEM STOP MERGES tab_int;
INSERT INTO tab_int SELECT number, 1000000 - number FROM numbers(6400);

SELECT '--- UInt32: reference';
SELECT k FROM tab_int ORDER BY k LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- UInt32: first and repeated runs';
SELECT k FROM tab_int ORDER BY k LIMIT 5 SETTINGS log_comment = '05565-int-first';
SELECT k FROM tab_int ORDER BY k LIMIT 5 SETTINGS log_comment = '05565-int-repeat';

-- The same with a `DateTime64` column.
CREATE TABLE tab_dt64 (id UInt32, t DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0,
    auto_statistics_types = '';
SYSTEM STOP MERGES tab_dt64;
INSERT INTO tab_dt64 SELECT number, fromUnixTimestamp64Milli(toInt64(1600000000000 - number), 'UTC') FROM numbers(6400);

SELECT '--- DateTime64: reference';
SELECT t FROM tab_dt64 ORDER BY t LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- DateTime64: first and repeated runs';
SELECT t FROM tab_dt64 ORDER BY t LIMIT 5 SETTINGS log_comment = '05565-dt64-first';
SELECT t FROM tab_dt64 ORDER BY t LIMIT 5 SETTINGS log_comment = '05565-dt64-repeat';

-- A `String` column in descending order, compared by unsigned bytes: each granule has one UTF-8 value, which sorts
-- after all ASCII values, and 63 ASCII values. The last 5 granules hold the result.
CREATE TABLE tab_str (id UInt32, s String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0,
    auto_statistics_types = '';
SYSTEM STOP MERGES tab_str;
INSERT INTO tab_str SELECT number, concat(if(number % 64 = 0, 'я', 'z'), leftPad(toString(number), 6, '0')) FROM numbers(6400);

SELECT '--- String DESC: reference';
SELECT s FROM tab_str ORDER BY s DESC LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- String DESC: first and repeated runs';
SELECT s FROM tab_str ORDER BY s DESC LIMIT 5 SETTINGS log_comment = '05565-str-first';
SELECT s FROM tab_str ORDER BY s DESC LIMIT 5 SETTINGS log_comment = '05565-str-repeat';

-- Ties: the final threshold value 0 is in every granule, so no granule can be dropped.
CREATE TABLE tab_ties (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0,
    auto_statistics_types = '';
SYSTEM STOP MERGES tab_ties;
INSERT INTO tab_ties SELECT number, if(number % 64 = 63, 0, 1000000 - number) FROM numbers(6400);

SELECT '--- ties: reference';
SELECT k, count() FROM (SELECT k FROM tab_ties ORDER BY k LIMIT 5) GROUP BY k SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- ties: first and repeated runs';
SELECT k, count() FROM (SELECT k FROM tab_ties ORDER BY k LIMIT 5) GROUP BY k SETTINGS log_comment = '05565-ties-first';
SELECT k, count() FROM (SELECT k FROM tab_ties ORDER BY k LIMIT 5) GROUP BY k SETTINGS log_comment = '05565-ties-repeat';

-- `GROUP BY g LIMIT 5`: the first 50 granules have one decreasing key each, the 5 smallest keys are spread over the
-- last 50. The aggregates of the returned groups, also the one equal to the threshold, need all their rows.
CREATE TABLE tab_group (id UInt32, g UInt32, v UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0,
    auto_statistics_types = '';
SYSTEM STOP MERGES tab_group;
INSERT INTO tab_group SELECT number, if(number < 3200, 1000 - intDiv(number, 64), number % 5), number FROM numbers(6400);

SELECT '--- GROUP BY LIMIT: reference';
SELECT g, count(), sum(v) FROM tab_group GROUP BY g ORDER BY g LIMIT 5
SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0, enable_group_by_top_k_optimization = 0;
SELECT '--- GROUP BY LIMIT: first and repeated runs';
SELECT * FROM (SELECT g, count(), sum(v) FROM tab_group GROUP BY g LIMIT 5) ORDER BY g SETTINGS log_comment = '05565-group-first';
SELECT * FROM (SELECT g, count(), sum(v) FROM tab_group GROUP BY g LIMIT 5) ORDER BY g SETTINGS log_comment = '05565-group-repeat';

-- A part that does not store the sort column computes it from its `DEFAULT`, which a later `ALTER` changes without
-- renaming the part. The first 50 granules hold 1000 on the first run, equal to the threshold of the first block, so
-- `__topKFilter` empties none of them; they hold 0 after the `ALTER`. The computed DEFAULT also turns the new writes
-- off, so the repeated run before the ALTER reads all rows.
CREATE TABLE tab_default (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0,
    auto_statistics_types = '';
SYSTEM STOP MERGES tab_default;
INSERT INTO tab_default SELECT number FROM numbers(6400);
ALTER TABLE tab_default ADD COLUMN v UInt32 DEFAULT if(id < 3200, 1000, id % 7 + 1);

SELECT '--- DEFAULT column: first and repeated runs';
SELECT v FROM tab_default ORDER BY v LIMIT 5 SETTINGS log_comment = '05565-default-first';
SELECT v FROM tab_default ORDER BY v LIMIT 5 SETTINGS log_comment = '05565-default-repeat';
ALTER TABLE tab_default MODIFY COLUMN v UInt32 DEFAULT if(id < 3200, 0, id % 7 + 1);
SELECT '--- DEFAULT column: reference after ALTER';
SELECT v FROM tab_default ORDER BY v LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- DEFAULT column: repeated run after ALTER';
SELECT v FROM tab_default ORDER BY v LIMIT 5;

-- A part that does not store `v` reads the type's default 0, and a later `DEFAULT` changes it without renaming the
-- part: the repeated run after the `ALTER` must not reuse the granules recorded with 0.
CREATE TABLE tab_alter (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0,
    auto_statistics_types = '';
SYSTEM STOP MERGES tab_alter;
INSERT INTO tab_alter SELECT number FROM numbers(6400);
ALTER TABLE tab_alter ADD COLUMN v UInt32;
INSERT INTO tab_alter SELECT 6400 + number, 1 + number FROM numbers(64);

SELECT '--- column added without DEFAULT: first and repeated runs';
SELECT v FROM tab_alter ORDER BY v DESC LIMIT 5 SETTINGS log_comment = '05565-alter-first';
SELECT v FROM tab_alter ORDER BY v DESC LIMIT 5 SETTINGS log_comment = '05565-alter-repeat';
ALTER TABLE tab_alter MODIFY COLUMN v UInt32 DEFAULT 1000;
SELECT '--- column added without DEFAULT: reference after ALTER';
SELECT v FROM tab_alter ORDER BY v DESC LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT '--- column added without DEFAULT: repeated run after ALTER';
SELECT v FROM tab_alter ORDER BY v DESC LIMIT 5;

SYSTEM FLUSH LOGS query_log;
SELECT '--- rows read';
SELECT log_comment, read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05565-%' AND log_comment NOT LIKE '05565-group-%'
ORDER BY event_time_microseconds
SETTINGS max_block_size = 65409, max_threads = 0;
-- The aggregation publishes its threshold when it trims its heap, so the final threshold depends on when that happens.
SELECT '--- GROUP BY LIMIT: the repeated run reads fewer rows';
SELECT log_comment, read_rows < 6400
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05565-group-%'
ORDER BY event_time_microseconds
SETTINGS max_block_size = 65409, max_threads = 0;

DROP TABLE tab_int;
DROP TABLE tab_dt64;
DROP TABLE tab_str;
DROP TABLE tab_ties;
DROP TABLE tab_group;
DROP TABLE tab_default;
DROP TABLE tab_alter;
