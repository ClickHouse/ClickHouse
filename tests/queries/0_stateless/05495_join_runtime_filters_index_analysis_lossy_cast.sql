-- A join key under a cast that can change values (e.g. narrowing) must not be used to prune granules by its
-- uncast values: `CAST(300 AS UInt8)` is 44, so the row 300 matches 44 and its granule has to be read.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_join_swap_table = 'false';
SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_rf_lossy_cast;
CREATE TABLE t_rf_lossy_cast (id UInt32)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_rf_lossy_cast;
INSERT INTO t_rf_lossy_cast SELECT number FROM numbers(512);

-- Narrowing cast of the global row index: exact values and range.
SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt8) = b.x ORDER BY a.id
SETTINGS log_comment = '05495_global_row_index_narrowing';

SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt8) = b.x ORDER BY a.id
SETTINGS join_runtime_filter_exact_values_limit = 0, log_comment = '05495_global_row_index_narrowing_range';

-- Narrowing cast of a primary key column: exact values and range.
SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a.id AS UInt8) = b.x ORDER BY a.id
SETTINGS log_comment = '05495_primary_key_narrowing';

SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a.id AS UInt8) = b.x ORDER BY a.id
SETTINGS join_runtime_filter_exact_values_limit = 0, log_comment = '05495_primary_key_narrowing_range';

-- A widening cast does not change values, so the key still prunes.
SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt128(300) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt128) = b.x ORDER BY a.id
SETTINGS log_comment = '05495_global_row_index_widening';

SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(300) AS x) AS b
WHERE a.id = b.x ORDER BY a.id
SETTINGS log_comment = '05495_primary_key_widening';

-- An unsigned key widened into a larger signed type keeps every value too: this is the common type of an unsigned and
-- a signed join key.
SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toInt64(300) AS x) AS b
WHERE a.id = b.x ORDER BY a.id
SETTINGS log_comment = '05495_primary_key_unsigned_to_signed';

SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toInt128(300) AS x) AS b
WHERE a._part_starting_offset + a._part_offset = b.x ORDER BY a.id
SETTINGS log_comment = '05495_global_row_index_unsigned_to_signed';

-- A date or time widened into a type with a range that contains it keeps every value: `Date` to `Date32` and
-- `DateTime` to `DateTime64`. `Date` to `DateTime` does not (the range of `DateTime` ends earlier), so it does not prune.
DROP TABLE IF EXISTS t_rf_lossy_cast_date;
DROP TABLE IF EXISTS t_rf_lossy_cast_date_time;
CREATE TABLE t_rf_lossy_cast_date (d Date)
ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
CREATE TABLE t_rf_lossy_cast_date_time (t DateTime('UTC'))
ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
INSERT INTO t_rf_lossy_cast_date SELECT toDate(number) FROM numbers(512);
INSERT INTO t_rf_lossy_cast_date_time SELECT toDateTime(number * 60, 'UTC') FROM numbers(512);

SELECT a.d FROM t_rf_lossy_cast_date AS a, (SELECT toDate32('1970-04-01') AS x) AS b
WHERE a.d = b.x ORDER BY a.d
SETTINGS log_comment = '05495_date_to_date32';

SELECT a.t FROM t_rf_lossy_cast_date_time AS a, (SELECT toDateTime64('1970-01-01 01:30:00', 3, 'UTC') AS x) AS b
WHERE a.t = b.x ORDER BY a.t
SETTINGS log_comment = '05495_date_time_to_date_time64';

SELECT a.d FROM t_rf_lossy_cast_date AS a, (SELECT toDateTime('1970-04-01 00:00:00', 'UTC') AS x) AS b
WHERE a.d = b.x ORDER BY a.d
SETTINGS session_timezone = 'UTC', log_comment = '05495_date_to_date_time';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterIndexAnalysisReads'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05495_global_row_index_narrowing', '05495_global_row_index_narrowing_range',
        '05495_primary_key_narrowing', '05495_primary_key_narrowing_range', '05495_global_row_index_widening',
        '05495_primary_key_widening', '05495_primary_key_unsigned_to_signed', '05495_global_row_index_unsigned_to_signed',
        '05495_date_to_date32', '05495_date_time_to_date_time64', '05495_date_to_date_time')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_rf_lossy_cast;
DROP TABLE t_rf_lossy_cast_date;
DROP TABLE t_rf_lossy_cast_date_time;
