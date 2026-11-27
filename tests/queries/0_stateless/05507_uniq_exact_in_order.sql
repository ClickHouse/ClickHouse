-- Random settings limits: index_granularity=(100, None); index_granularity_bytes=(100000, None)
-- A single uniqExact without GROUP BY over a table sorted by its argument counts the changes of the sorted values when optimize_aggregation_in_order is enabled.
-- One thread, as the sorted streams of several threads are counted in order only when the read is split into layers (below).
SET optimize_aggregation_in_order = 1, enable_parallel_replicas = 0, count_distinct_optimization = 0, max_threads = 1;

DROP TABLE IF EXISTS t_uint;
CREATE TABLE t_uint (x UInt64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES t_uint;
INSERT INTO t_uint SELECT number % 1000 FROM numbers(10000);
INSERT INTO t_uint SELECT number % 1500 FROM numbers(10000);
INSERT INTO t_uint SELECT number + 1400 FROM numbers(200);

SELECT 'uint', uniqExact(x), countDistinct(x), (SELECT uniqExact(x) FROM t_uint SETTINGS optimize_aggregation_in_order = 0) FROM t_uint;

SELECT 'plan', count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_uint) WHERE explain LIKE '%UniqExactInOrderTransform%';
SELECT 'plan off', count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_uint SETTINGS optimize_aggregation_in_order = 0) WHERE explain LIKE '%UniqExactInOrderTransform%';

DROP TABLE IF EXISTS t_other;
CREATE TABLE t_other (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_other SELECT number, number % 100 FROM numbers(1000);
SELECT 'not sorted', uniqExact(x), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_other) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_other;

SELECT 'where', uniqExact(x) FROM t_uint WHERE x >= 1000;
SELECT 'expression', uniqExact(intDiv(x, 7)) FROM t_uint;

DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY x SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES t_nullable;
INSERT INTO t_nullable SELECT if(number % 10 = 0, NULL, number % 1000) FROM numbers(10000);
INSERT INTO t_nullable SELECT if(number % 10 = 0, NULL, number % 1500) FROM numbers(10000);
INSERT INTO t_nullable SELECT number % 300 FROM numbers(1000);
SELECT 'nullable', uniqExact(x), (SELECT uniqExact(x) FROM t_nullable SETTINGS optimize_aggregation_in_order = 0) FROM t_nullable;

DROP TABLE IF EXISTS t_string;
CREATE TABLE t_string (s String) ENGINE = MergeTree ORDER BY s;
SYSTEM STOP MERGES t_string;
INSERT INTO t_string SELECT toString(number % 777) FROM numbers(10000);
INSERT INTO t_string SELECT toString(number % 999) FROM numbers(10000);
SELECT 'string', uniqExact(s), (SELECT uniqExact(s) FROM t_string SETTINGS optimize_aggregation_in_order = 0) FROM t_string;

DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s;
SYSTEM STOP MERGES t_lc;
INSERT INTO t_lc SELECT toString(number % 777) FROM numbers(10000);
INSERT INTO t_lc SELECT toString(number % 999) FROM numbers(10000);
SELECT 'low cardinality', uniqExact(s), (SELECT uniqExact(s) FROM t_lc SETTINGS optimize_aggregation_in_order = 0) FROM t_lc;

-- Floats are compared by bits, like uniqExact.
DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float (x Float64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES t_float;
-- 0.0, -0.0 and two NaNs with different bits, written by their bits.
INSERT INTO t_float SELECT arrayJoin([0., reinterpretAsFloat64(toUInt64(9223372036854775808)), 1., reinterpretAsFloat64(toUInt64(9221120237041090560)), 2.]);
INSERT INTO t_float SELECT arrayJoin([reinterpretAsFloat64(toUInt64(9223372036854775808)), reinterpretAsFloat64(toUInt64(9221120237041090561)), 1., 0., 3.]);
INSERT INTO t_float SELECT number % 100 FROM numbers(1000);
SELECT 'float', uniqExact(x), (SELECT uniqExact(x) FROM t_float SETTINGS optimize_aggregation_in_order = 0, optimize_uniq_exact_radix_partitioning = 0) FROM t_float;

DROP TABLE IF EXISTS t_float_nullable;
CREATE TABLE t_float_nullable (x Nullable(Float32)) ENGINE = MergeTree ORDER BY x SETTINGS allow_nullable_key = 1;
SYSTEM STOP MERGES t_float_nullable;
INSERT INTO t_float_nullable SELECT arrayJoin([0., reinterpretAsFloat32(toUInt32(2147483648)), NULL, reinterpretAsFloat32(toUInt32(2143289344)), 2.]);
INSERT INTO t_float_nullable SELECT arrayJoin([NULL, reinterpretAsFloat32(toUInt32(2143289345)), reinterpretAsFloat32(toUInt32(2147483648)), 5.]);
SELECT 'float nullable', uniqExact(x), (SELECT uniqExact(x) FROM t_float_nullable SETTINGS optimize_aggregation_in_order = 0, optimize_uniq_exact_radix_partitioning = 0) FROM t_float_nullable;

DROP TABLE IF EXISTS t_empty;
CREATE TABLE t_empty (x UInt64) ENGINE = MergeTree ORDER BY x;
SELECT 'empty', uniqExact(x) FROM t_empty;
SELECT 'empty result for empty set';
SELECT uniqExact(x) FROM t_empty SETTINGS empty_result_for_aggregation_by_empty_set = 1;

-- The distinct values are not kept, so the count of 1 million distinct values fits into a small memory limit without spilling.
DROP TABLE IF EXISTS t_memory;
CREATE TABLE t_memory (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_memory SELECT number FROM numbers(1000000);
SELECT 'memory', uniqExact(x) FROM t_memory SETTINGS max_threads = 1, max_memory_usage = '8Mi', max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, max_block_size = 8192;

DROP TABLE t_uint;
DROP TABLE t_other;
DROP TABLE t_nullable;
DROP TABLE t_string;
DROP TABLE t_lc;
DROP TABLE t_float;
DROP TABLE t_float_nullable;
DROP TABLE t_empty;
DROP TABLE t_memory;

-- Other types: the result matches the ordinary path, and the last column shows whether the changes are counted.
-- A float inside a `Tuple`, an `Array` or a `LowCardinality` is compared by value when sorted, but by bits by `uniqExact`
-- (0.0 and -0.0 differ), so such arguments are aggregated as usual.
DROP TABLE IF EXISTS t_types;
CREATE TABLE t_types
(
    k UInt64,
    i128 Int128,
    d Decimal(38, 5),
    u UUID,
    ip IPv6,
    fs FixedString(4),
    lcn LowCardinality(Nullable(String)),
    tup Tuple(UInt32, String),
    arr Array(UInt16),
    bf BFloat16,
    tup_f Tuple(Float64, UInt8),
    arr_f Array(Float64)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_types SELECT
    number,
    toInt128(number % 1700) * toInt128(1000000000000000000),
    toDecimal128(number % 1700, 5) / 7,
    reinterpretAsUUID(toUInt128(number % 1700)),
    toIPv6(toIPv4(number % 1700)),
    toFixedString(toString(number % 1700), 4),
    if(number % 7 = 0, NULL, toString(number % 1700)),
    (number % 50, toString(number % 37)),
    range(number % 20),
    if(number % 3 = 0, toBFloat16(-0.), toBFloat16(number % 100)),
    (if(number % 2 = 0, 0., -0.), number % 3),
    [if(number % 2 = 0, 0., -0.)]
FROM numbers(4000);

DROP TABLE IF EXISTS t_sorted;
CREATE TABLE t_sorted (x Int128) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT i128 FROM t_types;
SELECT 'Int128', uniqExact(x), (SELECT uniqExact(i128) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x Decimal(38, 5)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT d FROM t_types;
SELECT 'Decimal', uniqExact(x), (SELECT uniqExact(d) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x UUID) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT u FROM t_types;
SELECT 'UUID', uniqExact(x), (SELECT uniqExact(u) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x IPv6) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT ip FROM t_types;
SELECT 'IPv6', uniqExact(x), (SELECT uniqExact(ip) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x FixedString(4)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT fs FROM t_types;
SELECT 'FixedString', uniqExact(x), (SELECT uniqExact(fs) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY x SETTINGS allow_nullable_key = 1;
INSERT INTO t_sorted SELECT lcn FROM t_types;
SELECT 'LowCardinality(Nullable)', uniqExact(x), (SELECT uniqExact(lcn) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x Tuple(UInt32, String)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT tup FROM t_types;
SELECT 'Tuple', uniqExact(x), (SELECT uniqExact(tup) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x Array(UInt16)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT arr FROM t_types;
SELECT 'Array', uniqExact(x), (SELECT uniqExact(arr) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x BFloat16) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT bf FROM t_types;
SELECT 'BFloat16', uniqExact(x), (SELECT uniqExact(bf) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x Tuple(Float64, UInt8)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT tup_f FROM t_types;
SELECT 'Tuple with a float', uniqExact(x), (SELECT uniqExact(tup_f) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
CREATE TABLE t_sorted (x Array(Float64)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sorted SELECT arr_f FROM t_types;
SELECT 'Array of floats', uniqExact(x), (SELECT uniqExact(arr_f) FROM t_types), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_sorted) WHERE explain LIKE '%UniqExactInOrderTransform%') FROM t_sorted;
DROP TABLE t_sorted;
DROP TABLE t_types;

-- 0.0 and -0.0 of a column repeated by ARRAY JOIN, and of a LowCardinality(Float64), count as two values, as in uniqExact.
DROP TABLE IF EXISTS t_float_array_join;
CREATE TABLE t_float_array_join (x Nullable(Float64), arr Array(UInt8)) ENGINE = MergeTree ORDER BY x SETTINGS allow_nullable_key = 1;
INSERT INTO t_float_array_join SELECT reinterpretAsFloat64(arrayJoin([toUInt64(0), toUInt64(9223372036854775808)])), [1, 2, 3];
SELECT 'float array join', uniqExact(x) FROM t_float_array_join ARRAY JOIN arr SETTINGS enable_lazy_columns_replication = 1, max_threads = 1;
DROP TABLE t_float_array_join;

DROP TABLE IF EXISTS t_float_low_cardinality;
CREATE TABLE t_float_low_cardinality (x LowCardinality(Float64)) ENGINE = MergeTree ORDER BY x SETTINGS allow_suspicious_low_cardinality_types = 1;
SYSTEM STOP MERGES t_float_low_cardinality;
INSERT INTO t_float_low_cardinality SELECT reinterpretAsFloat64(toUInt64(9223372036854775808));
INSERT INTO t_float_low_cardinality SELECT reinterpretAsFloat64(toUInt64(0));
INSERT INTO t_float_low_cardinality SELECT reinterpretAsFloat64(toUInt64(9221120237041090561));
INSERT INTO t_float_low_cardinality SELECT reinterpretAsFloat64(toUInt64(9221120237041090562));
SELECT 'float low cardinality', uniqExact(x), (SELECT uniqExact(x) FROM t_float_low_cardinality SETTINGS optimize_aggregation_in_order = 0) FROM t_float_low_cardinality;
DROP TABLE t_float_low_cardinality;

-- With several threads the read is split into layers by the key, so that no value is in two layers, and the layers are counted in parallel.
DROP TABLE IF EXISTS t_layers;
CREATE TABLE t_layers (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_layers;
INSERT INTO t_layers SELECT number % 10000 FROM numbers(30000);
INSERT INTO t_layers SELECT number % 15000 FROM numbers(30000);
INSERT INTO t_layers SELECT (number * 7) % 20011 FROM numbers(30000);
SELECT 'layers', uniqExact(x), (SELECT uniqExact(x) FROM t_layers SETTINGS optimize_aggregation_in_order = 0) FROM t_layers SETTINGS max_threads = 8;
SELECT 'layers used', count() FROM (EXPLAIN actions = 1 SELECT uniqExact(x) FROM t_layers SETTINGS max_threads = 8) WHERE explain LIKE '%(in layers)%';
-- A function of the key can map values of two layers to the same value, so the read is not split.
SELECT 'layers function', uniqExact(intDiv(x, 7)), (SELECT count() FROM (EXPLAIN actions = 1 SELECT uniqExact(intDiv(x, 7)) FROM t_layers SETTINGS max_threads = 8) WHERE explain LIKE '%(in layers)%') FROM t_layers SETTINGS max_threads = 8;
DROP TABLE t_layers;

-- The layers are split by the key prefix that ends at the argument, here (a, x) with a fixed by the filter.
DROP TABLE IF EXISTS t_layers_prefix;
CREATE TABLE t_layers_prefix (a UInt8, x String, y UInt64) ENGINE = MergeTree ORDER BY (a, x, y) SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_layers_prefix;
INSERT INTO t_layers_prefix SELECT number % 3, toString(number % 500), number FROM numbers(20000);
INSERT INTO t_layers_prefix SELECT number % 3, toString(number % 700), number FROM numbers(20000);
SELECT 'layers prefix', uniqExact(x), (SELECT count() FROM (EXPLAIN actions = 1 SELECT uniqExact(x) FROM t_layers_prefix WHERE a = 1 SETTINGS max_threads = 8) WHERE explain LIKE '%(in layers)%') FROM t_layers_prefix WHERE a = 1 SETTINGS max_threads = 8;
DROP TABLE t_layers_prefix;

-- FINAL reads are not split into layers.
DROP TABLE IF EXISTS t_layers_final;
CREATE TABLE t_layers_final (x UInt64, v UInt8) ENGINE = ReplacingMergeTree ORDER BY x SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_layers_final;
INSERT INTO t_layers_final SELECT number % 10000, 1 FROM numbers(30000);
INSERT INTO t_layers_final SELECT number % 15000, 2 FROM numbers(30000);
SELECT 'layers final', uniqExact(x), (SELECT count() FROM (EXPLAIN actions = 1 SELECT uniqExact(x) FROM t_layers_final FINAL SETTINGS max_threads = 8) WHERE explain LIKE '%(in layers)%') FROM t_layers_final FINAL SETTINGS max_threads = 8;
DROP TABLE t_layers_final;

-- A projection that the filter makes cheaper to read is still chosen; it is not sorted by the argument.
DROP TABLE IF EXISTS t_projection;
CREATE TABLE t_projection (x UInt64, y UInt64, PROJECTION p (SELECT * ORDER BY y)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 128;
INSERT INTO t_projection SELECT number, (number * 7919) % 20000 FROM numbers(20000);
SELECT 'projection', uniqExact(x) FROM t_projection WHERE y = 4370 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'projection plan', count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_projection WHERE y = 4370 SETTINGS optimize_use_projections = 1) WHERE explain LIKE '%UniqExactInOrderTransform%';
DROP TABLE t_projection;

-- Without layers (floats, here), several streams would be merged into one on a single thread, so they are aggregated as usual,
-- and the table is not read in order.
DROP TABLE IF EXISTS t_no_layers;
CREATE TABLE t_no_layers (x Float64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_no_layers;
INSERT INTO t_no_layers SELECT toFloat64(number * 4) FROM numbers(10000);
INSERT INTO t_no_layers SELECT toFloat64(number * 4 + 1) FROM numbers(10000);
INSERT INTO t_no_layers SELECT toFloat64(number * 4 + 2) FROM numbers(10000);
INSERT INTO t_no_layers SELECT toFloat64(number * 4 + 3) FROM numbers(10000);
SELECT 'no layers', uniqExact(x), (SELECT count() FROM (EXPLAIN PIPELINE SELECT uniqExact(x) FROM t_no_layers SETTINGS max_threads = 8, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0) WHERE explain LIKE '%UniqExactInOrderTransform%' OR explain LIKE '%InOrder%') FROM t_no_layers SETTINGS max_threads = 8;
DROP TABLE t_no_layers;
