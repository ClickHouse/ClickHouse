-- A window finds its partitions with `compareAt`, under which -0. equals 0. and all NaNs are equal
-- to each other. The sort before the window scatters the rows by a hash of one representation of
-- such values, and must not reuse a partitioning that separates them.

DROP TABLE IF EXISTS t_window_float_nan;
DROP TABLE IF EXISTS t_window_float_distinct;
DROP TABLE IF EXISTS t_window_float_partitioned;
DROP TABLE IF EXISTS t_window_float_layers;

-- NaNs of both signs and with payloads. The table is sorted by the key, so the window reads it in
-- order and scatters the merged stream.
CREATE TABLE t_window_float_nan (k Float64, f Float32, v UInt64) ENGINE = MergeTree() ORDER BY k;

SYSTEM STOP MERGES t_window_float_nan;

INSERT INTO t_window_float_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);
INSERT INTO t_window_float_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);
INSERT INTO t_window_float_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);
INSERT INTO t_window_float_nan SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(9221120237041090561)), reinterpretAsFloat64(toUInt64(18444492273895866368)), reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 6 + 1], reinterpretAsFloat32(toUInt32([0, 2147483648, 2143289344, 2143289345, 4290772992, 4290772993][number % 6 + 1])), number FROM numbers(25000);

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_float_nan) ORDER BY c
SETTINGS max_threads = 8;

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_float_nan) ORDER BY c
SETTINGS max_threads = 1;

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY f) AS c FROM t_window_float_nan) ORDER BY c
SETTINGS max_threads = 8;

-- The Float32 key keeps the parallel scattered sort.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT count() OVER (PARTITION BY f) FROM t_window_float_nan SETTINGS max_threads = 8)
WHERE explain ILIKE '%ScatterByPartition%';

-- A distributed plan keeps the window above a sorted gather, which merges the streams of the sort below it.
SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_float_nan) ORDER BY c
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_optimize_exchanges = 1, distributed_plan_max_rows_to_broadcast = 0, enable_parallel_replicas = 0,
    distributed_plan_default_reader_bucket_count = 2, max_rows_to_group_by = 0, max_threads = 4, max_block_size = 1024,
    optimize_read_in_order = 1, query_plan_reuse_storage_ordering_for_window_functions = 1,
    read_in_order_use_buffering = 0, read_in_order_use_virtual_row = 0;

-- A parallel DISTINCT below the window partitions its streams by the bits of the key, which the
-- window must not reuse.
CREATE TABLE t_window_float_distinct (k Float64, v UInt64) ENGINE = MergeTree() ORDER BY v;

SYSTEM STOP MERGES t_window_float_distinct;

INSERT INTO t_window_float_distinct SELECT if(number % 2 = 0, 0., -0.), number FROM numbers(25000);
INSERT INTO t_window_float_distinct SELECT if(number % 2 = 0, 0., -0.), number FROM numbers(25000);
INSERT INTO t_window_float_distinct SELECT if(number % 2 = 0, 0., -0.), number FROM numbers(25000);
INSERT INTO t_window_float_distinct SELECT if(number % 2 = 0, 0., -0.), number FROM numbers(25000);

SELECT (SELECT groupArraySorted(16)(c) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT DISTINCT k FROM t_window_float_distinct))
        SETTINGS max_threads = 8, allow_parallel_distinct = 1)
     = (SELECT groupArraySorted(16)(c) FROM (SELECT count() OVER (PARTITION BY k) AS c FROM (SELECT DISTINCT k FROM t_window_float_distinct))
        SETTINGS max_threads = 1);

-- Equal floats can be in different partitions of the table, which the window must not reuse.
CREATE TABLE t_window_float_partitioned (k Float64, v UInt64) ENGINE = MergeTree() PARTITION BY k ORDER BY v
SETTINGS allow_floating_point_partition_key = 1;

INSERT INTO t_window_float_partitioned SELECT [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][number % 4 + 1], number FROM numbers(20000);

SELECT DISTINCT c FROM (SELECT count() OVER (PARTITION BY k) AS c FROM t_window_float_partitioned) ORDER BY c
SETTINGS max_threads = 8, allow_window_partitions_independently = 1, force_window_partitions_independently = 1;

SELECT count() FROM (EXPLAIN actions = 1 SELECT count() OVER (PARTITION BY k) FROM t_window_float_partitioned
    SETTINGS max_threads = 8, allow_window_partitions_independently = 1, force_window_partitions_independently = 1)
WHERE explain LIKE '%Read each partition through separate port%';

-- A window read by layers of the primary key keeps a partition in one layer.
CREATE TABLE t_window_float_layers (a UInt8, k Float64, v UInt64) ENGINE = MergeTree() ORDER BY (a, k);

SYSTEM STOP MERGES t_window_float_layers;

INSERT INTO t_window_float_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);
INSERT INTO t_window_float_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);
INSERT INTO t_window_float_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);
INSERT INTO t_window_float_layers SELECT number % 4, [0., -0., nan, reinterpretAsFloat64(toUInt64(18444492273895866369))][intDiv(number, 4) % 4 + 1], number FROM numbers(20000);

SELECT (SELECT groupArraySorted(64)(c) FROM (SELECT count() OVER (PARTITION BY a, k) AS c FROM t_window_float_layers) SETTINGS max_threads = 8)
     = (SELECT groupArraySorted(64)(c) FROM (SELECT count() OVER (PARTITION BY a, k) AS c FROM t_window_float_layers) SETTINGS max_threads = 1);

DROP TABLE t_window_float_nan;
DROP TABLE t_window_float_distinct;
DROP TABLE t_window_float_partitioned;
DROP TABLE t_window_float_layers;
