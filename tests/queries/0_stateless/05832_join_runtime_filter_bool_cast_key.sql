-- JOIN runtime filter index analysis must not drop rows when the probe key is cast to a type that `equals` the filter type,
-- e.g. `CAST(x AS Bool)` of a `UInt8` key: the pruning predicate has to apply the same cast as the filter.
-- https://github.com/ClickHouse/ClickHouse/issues/130429

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0, join_algorithm = 'hash';

DROP TABLE IF EXISTS probe_u8;
DROP TABLE IF EXISTS probe_u8_si;
DROP TABLE IF EXISTS probe_u8_null;
DROP TABLE IF EXISTS probe_u16;
DROP TABLE IF EXISTS build_true;
DROP TABLE IF EXISTS build_false;
DROP TABLE IF EXISTS build_u8;

CREATE TABLE probe_u8 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
INSERT INTO probe_u8 SELECT intDiv(number, 16) FROM numbers(4096);

CREATE TABLE probe_u8_si (id UInt64, x UInt8, INDEX idx_x x TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO probe_u8_si SELECT number, intDiv(number, 16) FROM numbers(4096);

CREATE TABLE probe_u8_null (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO probe_u8_null SELECT if(number % 1000 = 7, NULL, intDiv(number, 16)) FROM numbers(4096);

CREATE TABLE probe_u16 (x UInt16) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
INSERT INTO probe_u16 SELECT number FROM numbers(4096);

CREATE TABLE build_true (flag Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_true VALUES (true);

CREATE TABLE build_false (flag Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_false VALUES (false);

CREATE TABLE build_u8 (u UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_u8 VALUES (1);

-- Primary key, read-time pruning.
SELECT 'bool_read', count() FROM probe_u8 AS p JOIN build_true AS b ON CAST(p.x AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0,
    log_comment = '05832_bool_read';

-- Primary key, seal-gated reading.
SELECT 'bool_seal', count() FROM probe_u8 AS p JOIN build_true AS b ON CAST(p.x AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 0, enable_join_seal_gated_reading = 1;

-- minmax skip index on a non-primary-key column.
SELECT 'bool_skip_minmax', count() FROM probe_u8_si AS p JOIN build_true AS b ON CAST(p.x AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- A `Nullable(UInt8)` key cast to `Nullable(Bool)`.
SELECT 'bool_nullable', count() FROM probe_u8_null AS p JOIN build_true AS b ON CAST(p.x AS Nullable(Bool)) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- A `UInt8` build key: the filter type is `UInt8`, the probe key is still cast to `Bool`.
SELECT 'bool_u8_build', count() FROM probe_u8 AS p JOIN build_u8 AS b ON CAST(p.x AS Bool) = b.u
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- A `UInt16` key cast to `Bool`, compared with a `UInt8` build key.
SELECT 'u16_bool_u8_build', count() FROM probe_u16 AS p JOIN build_u8 AS b ON CAST(p.x AS Bool) = b.u
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- A `UInt16` key cast to `UInt8`, compared with a `Bool` build key.
SELECT 'u16_u8_bool_build', count() FROM probe_u16 AS p JOIN build_false AS b ON CAST(p.x AS UInt8) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

SYSTEM FLUSH LOGS query_log;

-- The cast to `Bool` is used for pruning.
SELECT 'bool_read_pruned', ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05832_bool_read' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE probe_u8;
DROP TABLE probe_u8_si;
DROP TABLE probe_u8_null;
DROP TABLE probe_u16;
DROP TABLE build_true;
DROP TABLE build_false;
DROP TABLE build_u8;
