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
