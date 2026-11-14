-- A window sorting that reads the table by primary key ranges (`query_plan_reuse_storage_ordering_for_window_functions`)
-- must keep every row: the range split stops before a key column that cannot be split into ranges, and it follows
-- the stored order of the key. It must also not read the sorting key columns the query does not need.

SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1;
SET allow_window_partitions_independently = 0;
SET max_threads = 4;

-- A computed floating-point key column with `NaN`: a range filter compares it as false against any border.
DROP TABLE IF EXISTS t_window_layers_nan;
CREATE TABLE t_window_layers_nan (a UInt32, f UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, if(f = 99, nan, toFloat64(f)), b) SETTINGS index_granularity = 100;
INSERT INTO t_window_layers_nan SELECT 0, number % 100, number FROM numbers(10000);
SELECT count(), countIf(f = 99), (SELECT sum(cityHash64(a, f, b, s)) FROM (SELECT a, f, b, sum(b) OVER (PARTITION BY a, f ORDER BY b) AS s FROM t_window_layers_nan) SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0) = (SELECT sum(cityHash64(a, f, b, s)) FROM (SELECT a, f, b, sum(b) OVER (PARTITION BY a, f ORDER BY b) AS s FROM t_window_layers_nan))
FROM (SELECT a, f, b, sum(b) OVER (PARTITION BY a, f ORDER BY b) AS s FROM t_window_layers_nan)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1;
DROP TABLE t_window_layers_nan;

-- A decreasing window key reads the parts in the reverse order of the key.
DROP TABLE IF EXISTS t_window_layers_reverse_read;
CREATE TABLE t_window_layers_reverse_read (a Int32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 100;
INSERT INTO t_window_layers_reverse_read SELECT number % 100, number FROM numbers(10000);
SELECT count(), (SELECT sum(cityHash64(a, b, s)) FROM (SELECT a, b, sum(b) OVER (PARTITION BY negate(a) ORDER BY b) AS s FROM t_window_layers_reverse_read) SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0) = (SELECT sum(cityHash64(a, b, s)) FROM (SELECT a, b, sum(b) OVER (PARTITION BY negate(a) ORDER BY b) AS s FROM t_window_layers_reverse_read))
FROM (SELECT a, b, sum(b) OVER (PARTITION BY negate(a) ORDER BY b) AS s FROM t_window_layers_reverse_read)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1;
DROP TABLE t_window_layers_reverse_read;

-- Key columns stored in different directions.
DROP TABLE IF EXISTS t_window_layers_mixed_directions;
CREATE TABLE t_window_layers_mixed_directions (a Int32, b UInt32) ENGINE = MergeTree ORDER BY (a, b DESC) SETTINGS index_granularity = 100;
INSERT INTO t_window_layers_mixed_directions SELECT number % 100, number % 200 FROM numbers(20000);
SELECT count(), (SELECT sum(cityHash64(a, b, rn)) FROM (SELECT a, b, count() OVER (PARTITION BY a, b) AS rn FROM t_window_layers_mixed_directions) SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0) = (SELECT sum(cityHash64(a, b, rn)) FROM (SELECT a, b, count() OVER (PARTITION BY a, b) AS rn FROM t_window_layers_mixed_directions))
FROM (SELECT a, b, count() OVER (PARTITION BY a, b) AS rn FROM t_window_layers_mixed_directions)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1;
DROP TABLE t_window_layers_mixed_directions;

-- Window keys `(a, b)` determine the key `(a, b % 97)`, but the read is ordered only by `a`: the layers are split by `a`.
DROP TABLE IF EXISTS t_window_layers_unordered_suffix;
CREATE TABLE t_window_layers_unordered_suffix (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b % 97) SETTINGS index_granularity = 100;
SYSTEM STOP MERGES t_window_layers_unordered_suffix;
INSERT INTO t_window_layers_unordered_suffix SELECT intDiv(number, 1000), number FROM numbers(10000);
INSERT INTO t_window_layers_unordered_suffix SELECT intDiv(number, 1000), number FROM numbers(10000);
SELECT count(), min(s), max(s)
FROM (SELECT count() OVER (PARTITION BY a, b) AS s FROM t_window_layers_unordered_suffix)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, max_block_size = 1000;
DROP TABLE t_window_layers_unordered_suffix;

-- A function of the key that depends on a setting: the table stores month names, the query computes minutes.
-- The window is not read in order by the key, which is another calculation.
DROP TABLE IF EXISTS t_window_layers_function_settings;
SET formatdatetime_parsedatetime_m_is_month_name = 1;
CREATE TABLE t_window_layers_function_settings (dt DateTime('UTC'), b UInt32) ENGINE = MergeTree
ORDER BY (formatDateTime(dt, '%M'), b) SETTINGS index_granularity = 100;
SYSTEM STOP MERGES t_window_layers_function_settings;
INSERT INTO t_window_layers_function_settings SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60, number FROM numbers(10000);
INSERT INTO t_window_layers_function_settings SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60, number FROM numbers(10000);
SET formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT count(), (SELECT sum(cityHash64(dt, b, w)) FROM (SELECT dt, b, count() OVER (PARTITION BY formatDateTime(dt, '%M')) AS w FROM t_window_layers_function_settings) SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0) = (SELECT sum(cityHash64(dt, b, w)) FROM (SELECT dt, b, count() OVER (PARTITION BY formatDateTime(dt, '%M')) AS w FROM t_window_layers_function_settings))
FROM (SELECT dt, b, count() OVER (PARTITION BY formatDateTime(dt, '%M')) AS w FROM t_window_layers_function_settings)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, max_block_size = 1000;
-- The raw column `dt` in `PARTITION BY` passes the layer keys, but the order still comes from the stored month names.
SELECT count(), (SELECT sum(cityHash64(dt, b, w)) FROM (SELECT dt, b, count() OVER (PARTITION BY formatDateTime(dt, '%M'), dt) AS w FROM t_window_layers_function_settings) SETTINGS optimize_read_in_order = 0) = (SELECT sum(cityHash64(dt, b, w)) FROM (SELECT dt, b, count() OVER (PARTITION BY formatDateTime(dt, '%M'), dt) AS w FROM t_window_layers_function_settings))
FROM (SELECT dt, b, count() OVER (PARTITION BY formatDateTime(dt, '%M'), dt) AS w FROM t_window_layers_function_settings)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, max_block_size = 1000;
-- A `UNION ALL` of a branch read in order and a branch sorted by a helper sorting, which feeds the window.
DROP TABLE IF EXISTS t_window_layers_function_settings_raw;
CREATE TABLE t_window_layers_function_settings_raw (k String, b UInt32) ENGINE = MergeTree ORDER BY (k, b) SETTINGS index_granularity = 100;
INSERT INTO t_window_layers_function_settings_raw SELECT formatDateTime(dt, '%M'), b FROM t_window_layers_function_settings;
SELECT count(), min(w), max(w) FROM (SELECT count() OVER (PARTITION BY k) AS w FROM (SELECT k, b FROM t_window_layers_function_settings_raw UNION ALL SELECT formatDateTime(dt, '%M') AS k, b FROM t_window_layers_function_settings))
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, max_block_size = 1000;
DROP TABLE t_window_layers_function_settings_raw;
DROP TABLE t_window_layers_function_settings;

-- Windows without scatter keys: a floating-point `PARTITION BY` key, and a window with only `ORDER BY`.
DROP TABLE IF EXISTS t_window_function_settings_no_scatter;
SET formatdatetime_parsedatetime_m_is_month_name = 1;
CREATE TABLE t_window_function_settings_no_scatter (dt DateTime('UTC'), f Float64, b UInt32) ENGINE = MergeTree
ORDER BY (formatDateTime(dt, '%M'), f, b) SETTINGS index_granularity = 100;
SYSTEM STOP MERGES t_window_function_settings_no_scatter;
INSERT INTO t_window_function_settings_no_scatter SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60, number % 2, number FROM numbers(10000);
INSERT INTO t_window_function_settings_no_scatter SELECT * FROM t_window_function_settings_no_scatter;
SET formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT count(), (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, count() OVER (PARTITION BY formatDateTime(dt, '%M'), f) AS w FROM t_window_function_settings_no_scatter) SETTINGS optimize_read_in_order = 0) = (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, count() OVER (PARTITION BY formatDateTime(dt, '%M'), f) AS w FROM t_window_function_settings_no_scatter))
FROM (SELECT dt, f, b, count() OVER (PARTITION BY formatDateTime(dt, '%M'), f) AS w FROM t_window_function_settings_no_scatter)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, max_block_size = 1000;
SELECT count(), (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter) SETTINGS optimize_read_in_order = 0) = (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter))
FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, max_block_size = 1000;
-- The same windows in a distributed plan, whose fragments are serialized and optimized again.
SELECT count(), min(w), max(w) FROM (SELECT count() OVER (PARTITION BY formatDateTime(dt, '%M'), f) AS w FROM t_window_function_settings_no_scatter)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0, max_block_size = 1000;
SELECT count(), (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter) SETTINGS optimize_read_in_order = 0) = (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter))
FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0, max_block_size = 1000;
-- The same windows with the Cascades optimizer, which rebuilds the sorting from the required properties.
SELECT count(), min(w), max(w) FROM (SELECT count() OVER (PARTITION BY formatDateTime(dt, '%M'), f) AS w FROM t_window_function_settings_no_scatter)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0, enable_cascades_optimizer = 1, distributed_plan_workers_num = 1, max_block_size = 1000;
SELECT count(), (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter) SETTINGS optimize_read_in_order = 0) = (SELECT sum(cityHash64(dt, f, b, w)) FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter))
FROM (SELECT dt, f, b, row_number() OVER (ORDER BY formatDateTime(dt, '%M'), f, b) AS w FROM t_window_function_settings_no_scatter)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0, enable_cascades_optimizer = 1, distributed_plan_workers_num = 1, max_block_size = 1000;
DROP TABLE t_window_function_settings_no_scatter;

-- The sorting key column `payload` is not used by the query and must not be read.
DROP TABLE IF EXISTS t_window_layers_unused_key_suffix;
CREATE TABLE t_window_layers_unused_key_suffix (a UInt32, payload String) ENGINE = MergeTree ORDER BY (a, payload) SETTINGS index_granularity = 100;
INSERT INTO t_window_layers_unused_key_suffix SELECT number % 100, repeat(toString(number), 200) FROM numbers(10000);
SELECT sum(c) FROM (SELECT a, count() OVER (PARTITION BY a) AS c FROM t_window_layers_unused_key_suffix)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1, log_comment = '05629_unused_key_suffix';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['SelectedBytes'] < 1000000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05629_unused_key_suffix' AND type = 'QueryFinish' AND query_kind = 'Select'
ORDER BY event_time_microseconds DESC
LIMIT 1;
DROP TABLE t_window_layers_unused_key_suffix;
