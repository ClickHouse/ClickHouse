-- JOIN runtime filter index analysis must not drop rows when the probe key is a narrowing CAST
-- nested under the analyzer's key-unification cast, e.g. `_CAST(CAST(k, 'UInt8'), 'UInt64')`.
-- https://github.com/ClickHouse/ClickHouse/issues/124679

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0, join_algorithm = 'hash';

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS probe_si;
DROP TABLE IF EXISTS probe_ts;
DROP TABLE IF EXISTS build64;
DROP TABLE IF EXISTS build8;
DROP TABLE IF EXISTS build_dt;

CREATE TABLE probe (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO probe SELECT number FROM numbers(4096);

CREATE TABLE probe_si (id UInt64, k UInt64, INDEX idx_k k TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO probe_si SELECT number, number FROM numbers(4096);

CREATE TABLE probe_ts (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 8;
INSERT INTO probe_ts SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 60 FROM numbers(4096);

CREATE TABLE build64 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO build64 VALUES (16);

CREATE TABLE build8 (k UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO build8 VALUES (16);

CREATE TABLE build_dt (d DateTime('UTC')) ENGINE = MergeTree ORDER BY d;
INSERT INTO build_dt VALUES (toDateTime('2024-01-02 00:00:00', 'UTC'));

-- Primary key, read-time pruning.
SELECT 'pk_read', count() FROM probe AS p JOIN build64 AS b ON CAST(p.k AS UInt8) = b.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- Primary key, seal-gated reading.
SELECT 'pk_seal', count() FROM probe AS p JOIN build64 AS b ON CAST(p.k AS UInt8) = b.k
SETTINGS enable_join_runtime_filters_index_analysis = 0, enable_join_seal_gated_reading = 1;

-- DateTime key cast to Date, compared with a DateTime build key.
SELECT 'date', count() FROM probe_ts AS p JOIN build_dt AS b ON CAST(p.ts AS Date) = b.d
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- minmax skip index on a non-primary-key column.
SELECT 'skip_minmax', count() FROM probe_si AS p JOIN build64 AS b ON CAST(p.k AS UInt8) = b.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- The narrowing cast is computed in a subquery.
SELECT 'subquery', count() FROM (SELECT CAST(k AS UInt8) AS k8 FROM probe) AS p JOIN build64 AS b ON p.k8 = b.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- A single cast to the build key type is exact, so it is still used for pruning.
SELECT 'single_cast', count() FROM probe AS p JOIN build8 AS b ON CAST(p.k AS UInt8) = b.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0,
    log_comment = '05457_single_cast';

SYSTEM FLUSH LOGS query_log;

SELECT 'single_cast_pruned', ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05457_single_cast' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE probe;
DROP TABLE probe_si;
DROP TABLE probe_ts;
DROP TABLE build64;
DROP TABLE build8;
DROP TABLE build_dt;
