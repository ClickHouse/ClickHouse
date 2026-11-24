-- JOIN runtime filter index analysis must not drop rows when the probe key is cast to a type that `equals` the filter type,
-- e.g. `CAST(x AS Bool)` of a `UInt8` key: the pruning predicate has to apply the same cast as the filter.
-- https://github.com/ClickHouse/ClickHouse/issues/130429

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0, join_algorithm = 'hash';
SET date_time_overflow_behavior = 'ignore';
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS probe_u8;
DROP TABLE IF EXISTS probe_u8_si;
DROP TABLE IF EXISTS probe_u8_null;
DROP TABLE IF EXISTS probe_u16;
DROP TABLE IF EXISTS probe_u16_set;
DROP TABLE IF EXISTS probe_bool_set;
DROP TABLE IF EXISTS probe_variant;
DROP TABLE IF EXISTS probe_time;
DROP TABLE IF EXISTS probe_dec;
DROP TABLE IF EXISTS probe_tuple;
DROP TABLE IF EXISTS probe_date32;
DROP TABLE IF EXISTS build_true;
DROP TABLE IF EXISTS build_false;
DROP TABLE IF EXISTS build_two;
DROP TABLE IF EXISTS build_two_three;
DROP TABLE IF EXISTS build_bool_both;
DROP TABLE IF EXISTS build_u8;
DROP TABLE IF EXISTS build_u8_two;
DROP TABLE IF EXISTS build_variant;
DROP TABLE IF EXISTS build_tokyo;
DROP TABLE IF EXISTS build_dec;
DROP TABLE IF EXISTS build_tuple;
DROP TABLE IF EXISTS build_overflow;

CREATE TABLE probe_u8 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
INSERT INTO probe_u8 SELECT intDiv(number, 16) FROM numbers(4096);

CREATE TABLE probe_u8_si (
    id UInt64, x UInt8, x_set UInt8, x_bf UInt8,
    INDEX idx_x x TYPE minmax GRANULARITY 1,
    INDEX idx_x_set x_set TYPE set(0) GRANULARITY 1,
    INDEX idx_x_bf x_bf TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO probe_u8_si SELECT number, intDiv(number, 16), intDiv(number, 16), intDiv(number, 16) FROM numbers(4096);

CREATE TABLE probe_u8_null (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8, allow_nullable_key = 1;
INSERT INTO probe_u8_null SELECT if(number % 1000 = 7, NULL, intDiv(number, 16)) FROM numbers(4096);

CREATE TABLE probe_u16 (x UInt16) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
INSERT INTO probe_u16 SELECT number FROM numbers(4096);

CREATE TABLE probe_u16_set (id UInt64, x UInt16, INDEX idx_x x TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO probe_u16_set SELECT number, number FROM numbers(4096);

-- `reinterpret` keeps the bytes other than 0 and 1 in a `Bool` column.
CREATE TABLE probe_bool_set (id UInt64, x Bool, INDEX idx_x x TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO probe_bool_set SELECT number, reinterpret(toUInt8(intDiv(number, 16)), 'Bool') FROM numbers(4096);

CREATE TABLE probe_variant (id UInt64, x Variant(Bool, UInt8), INDEX idx_x x TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO probe_variant SELECT number, if(number % 2, CAST(true, 'Variant(Bool, UInt8)'), CAST(toUInt8(intDiv(number, 16)), 'Variant(Bool, UInt8)'))
FROM numbers(4096);

CREATE TABLE probe_time (d Date, t DateTime('UTC'), INDEX idx_t t TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 8;
INSERT INTO probe_time SELECT toDate('2026-01-01') + intDiv(number, 16) AS d, toDateTime(d, 'UTC') FROM numbers(4096);

CREATE TABLE probe_dec (d Decimal(12, 2)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 8;
INSERT INTO probe_dec SELECT intDiv(number, 16) FROM numbers(4096);

CREATE TABLE probe_tuple (t Tuple(UInt8)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 8;
INSERT INTO probe_tuple SELECT tuple(intDiv(number, 16)) FROM numbers(4096);

CREATE TABLE probe_date32 (id UInt64, d Date32, INDEX idx_d d TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO probe_date32 SELECT number, if(number < 256, toDate32('2106-02-08'), toDate32('2000-01-01')) FROM numbers(512);

CREATE TABLE build_true (flag Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_true VALUES (true);

CREATE TABLE build_false (flag Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_false VALUES (false);

-- `RowBinary` keeps the byte 2 in a `Bool` column.
CREATE TABLE build_two (flag Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_two SELECT * FROM format(RowBinary, 'flag Bool', unhex('02'));

CREATE TABLE build_two_three (flag Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_two_three SELECT * FROM format(RowBinary, 'flag Bool', unhex('0203'));

CREATE TABLE build_bool_both (flag Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_bool_both VALUES (false), (true);

CREATE TABLE build_u8 (u UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_u8 VALUES (1);

CREATE TABLE build_u8_two (u UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_u8_two VALUES (2);

CREATE TABLE build_variant (k Variant(Bool, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_variant SELECT CAST(toUInt8(2), 'Variant(Bool, UInt8)');

CREATE TABLE build_tokyo (t DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_tokyo SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalDay(number) FROM numbers(3);

CREATE TABLE build_dec (d Decimal(18, 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_dec VALUES (5);

CREATE TABLE build_tuple (t Tuple(Bool)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_tuple VALUES (tuple(true));

CREATE TABLE build_overflow (t DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_overflow SELECT CAST(toDate32('2106-02-08') AS DateTime('UTC'));

-- Primary key, read-time pruning.
SELECT 'bool_read', count() FROM probe_u8 AS p JOIN build_true AS b ON CAST(p.x AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0,
    log_comment = '05832_bool_read';

-- The same with a range instead of the exact values.
SELECT 'bool_read_range', count() FROM probe_u8 AS p JOIN build_true AS b ON CAST(p.x AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0,
    join_runtime_filter_exact_values_limit = 0, log_comment = '05832_bool_read_range';

-- Primary key, seal-gated reading.
SELECT 'bool_seal', count() FROM probe_u8 AS p JOIN build_true AS b ON CAST(p.x AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 0, enable_join_seal_gated_reading = 1;

-- Skip indexes on non-primary-key columns.
SELECT 'bool_skip_minmax', count() FROM probe_u8_si AS p JOIN build_true AS b ON CAST(p.x AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;
SELECT 'bool_skip_set', count() FROM probe_u8_si AS p JOIN build_true AS b ON CAST(p.x_set AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;
SELECT 'bool_skip_bloom_filter', count() FROM probe_u8_si AS p JOIN build_true AS b ON CAST(p.x_bf AS Bool) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- A `Nullable(UInt8)` key cast to `Nullable(Bool)`.
SELECT 'bool_nullable', count() FROM probe_u8_null AS p JOIN build_true AS b ON CAST(p.x AS Nullable(Bool)) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- Casts to `SimpleAggregateFunction` of `Bool` or of `Tuple(Bool)` convert 2 to 1 as well.
SELECT 'bool_simple_aggregate_function', count() FROM probe_u8 AS p JOIN build_true AS b
    ON CAST(p.x AS SimpleAggregateFunction(any, Bool)) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;
SELECT 'tuple_bool_simple_aggregate_function', count() FROM probe_tuple AS p JOIN build_tuple AS b
    ON CAST(p.t AS SimpleAggregateFunction(any, Tuple(Bool))) = b.t
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

-- The same with the byte 2 in the `Bool` build key and a `set` index: the JOIN compares the bytes.
SELECT 'u16_u8_bool_byte_build_set', count() FROM probe_u16_set AS p JOIN build_two AS b ON CAST(p.x AS UInt8) = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    enable_join_seal_gated_reading = 0, log_comment = '05832_u16_u8_bool_byte_build_set';

-- A `Bool` key holding the byte 2, compared with a `UInt8` or a `Bool` build key holding 2, with a `set` index.
SELECT 'bool_byte_u8_byte_build_set', count() FROM probe_bool_set AS p JOIN build_u8_two AS b ON p.x = b.u
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    enable_join_seal_gated_reading = 0, log_comment = '05832_bool_byte_u8_byte_build_set';
SELECT 'bool_byte_bool_byte_build_set', count() FROM probe_bool_set AS p JOIN build_two AS b ON p.x = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    enable_join_seal_gated_reading = 0, log_comment = '05832_bool_byte_bool_byte_build_set';

-- The runtime filter itself, with two build values, so it looks the probe key up in a `Set`. Seal-gated reading
-- builds it before the probe side is read.
SELECT 'bool_byte_two_bytes_build_filter', count() FROM probe_bool_set AS p JOIN build_two_three AS b ON p.x = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 0, join_runtime_filter_from_fixed_hash_table = 0,
    enable_join_seal_gated_reading = 1, max_threads = 1, log_comment = '05832_bool_byte_two_bytes_build_filter';
SELECT 'bool_byte_anti_bool_build_filter', count() FROM probe_bool_set AS p LEFT ANTI JOIN build_bool_both AS b ON p.x = b.flag
SETTINGS enable_join_runtime_filters_index_analysis = 0, enable_join_seal_gated_reading = 1, max_threads = 1,
    log_comment = '05832_bool_byte_anti_bool_build_filter';

-- A `Variant` key holding both `Bool` and `UInt8`.
SELECT 'variant_bool_u8', count() FROM probe_variant AS p JOIN build_variant AS b ON p.x = b.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    enable_join_seal_gated_reading = 0;

-- A `Date` key cast to `DateTime('UTC')`, compared with a `DateTime('Asia/Tokyo')` build key.
SELECT 'date_utc_tokyo_build', count() FROM probe_time AS p JOIN build_tokyo AS b ON CAST(p.d AS DateTime('UTC')) = b.t
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0;

-- A `CAST` out of the `DateTime` range follows `date_time_overflow_behavior`, unlike the internal `_CAST`.
SELECT 'date32_overflow', count() FROM probe_date32 AS p JOIN build_overflow AS b ON CAST(p.d AS DateTime('UTC')) = b.t
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    enable_join_seal_gated_reading = 0, log_comment = '05832_date32_overflow';

-- Casts that keep the values still prune: a time zone change, with a `bloom_filter` index, and a `Decimal` precision change.
-- The `bloom_filter` index is probed only with the exact values of the runtime filter, so their limit is pinned.
SELECT 'datetime_time_zone', count() FROM probe_time AS p JOIN build_tokyo AS b ON CAST(p.t AS DateTime('Asia/Tokyo')) = b.t
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1,
    enable_join_seal_gated_reading = 0, join_runtime_filter_exact_values_limit = 10000, log_comment = '05832_datetime_time_zone';
SELECT 'decimal_precision', count() FROM probe_dec AS p JOIN build_dec AS b ON CAST(p.d AS Decimal(18, 2)) = b.d
SETTINGS enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1, enable_join_seal_gated_reading = 0,
    log_comment = '05832_decimal_precision';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05832_bool_byte_bool_byte_build_set', '05832_bool_byte_u8_byte_build_set', '05832_bool_read',
        '05832_bool_read_range', '05832_date32_overflow', '05832_datetime_time_zone', '05832_decimal_precision',
        '05832_u16_u8_bool_byte_build_set')
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['RuntimeFilterRowsChecked'] > 0
    AND ProfileEvents['RuntimeFilterRowsPassed'] < ProfileEvents['RuntimeFilterRowsChecked']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05832_bool_byte_anti_bool_build_filter', '05832_bool_byte_two_bytes_build_filter')
ORDER BY log_comment;

DROP TABLE probe_u8;
DROP TABLE probe_u8_si;
DROP TABLE probe_u8_null;
DROP TABLE probe_u16;
DROP TABLE probe_u16_set;
DROP TABLE probe_bool_set;
DROP TABLE probe_variant;
DROP TABLE probe_time;
DROP TABLE probe_dec;
DROP TABLE probe_tuple;
DROP TABLE probe_date32;
DROP TABLE build_true;
DROP TABLE build_false;
DROP TABLE build_two;
DROP TABLE build_two_three;
DROP TABLE build_bool_both;
DROP TABLE build_u8;
DROP TABLE build_u8_two;
DROP TABLE build_variant;
DROP TABLE build_tokyo;
DROP TABLE build_dec;
DROP TABLE build_tuple;
DROP TABLE build_overflow;
