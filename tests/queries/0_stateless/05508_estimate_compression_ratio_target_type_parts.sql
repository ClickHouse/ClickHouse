-- `estimateCompressionRatio` with a `target_type` against real parts: for the rows of one column, every candidate (type and
-- codec) is estimated and also stored. The estimates must rank the candidates as the parts do, and the estimated sizes must
-- be proportional to the stored ones within 2%. All the settings the estimate follows are pinned to their defaults.

-- A state split between threads or replicas is estimated as several parts.
SET max_threads = 1, max_insert_threads = 1, enable_parallel_replicas = 0;
SET max_insert_block_size = 1048576, min_insert_block_size_rows = 1048576, min_insert_block_size_bytes = 0;
SET low_cardinality_max_dictionary_size = 8192, low_cardinality_use_single_dictionary_for_part = 0;

DROP TABLE IF EXISTS t_strings;
DROP TABLE IF EXISTS t_unique;
DROP TABLE IF EXISTS t_ints;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_ip;
DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_arrays;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS est;

CREATE TABLE t_strings
(
    x String CODEC(LZ4),
    str_zstd String CODEC(ZSTD(3)),
    str_lz4 String CODEC(LZ4),
    lc_lz4 LowCardinality(String) CODEC(LZ4),
    lc_zstd LowCardinality(String) CODEC(ZSTD(3))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream', nullable_serialization_version = 'basic',
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic',
    propagate_types_serialization_versions_to_nested_types = 1, min_columns_to_activate_adaptive_write_buffer = 500;

CREATE TABLE t_unique AS t_strings;
CREATE TABLE t_wide AS t_strings;
CREATE TABLE t_ints
(
    x UInt64 CODEC(LZ4),
    u64_lz4 UInt64 CODEC(LZ4),
    u64_t64 UInt64 CODEC(T64, LZ4),
    u32_t64 UInt32 CODEC(T64, LZ4),
    u32_delta UInt32 CODEC(Delta, LZ4),
    u32_doubledelta UInt32 CODEC(DoubleDelta, ZSTD(3))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192,
    index_granularity_bytes = '10Mi', use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    min_columns_to_activate_adaptive_write_buffer = 500;
CREATE TABLE t_nullable
(
    x Nullable(String) CODEC(LZ4),
    nullable_zstd Nullable(String) CODEC(ZSTD(3)),
    nullable_lz4 Nullable(String) CODEC(LZ4),
    lc_nullable_zstd LowCardinality(Nullable(String)) CODEC(ZSTD(3))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192,
    index_granularity_bytes = '10Mi', use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream', nullable_serialization_version = 'basic',
    propagate_types_serialization_versions_to_nested_types = 1, min_columns_to_activate_adaptive_write_buffer = 500;
CREATE TABLE t_ip
(
    x IPv4 CODEC(LZ4),
    ipv4_lz4 IPv4 CODEC(LZ4),
    ipv6_lz4 IPv6 CODEC(LZ4),
    ipv6_zstd IPv6 CODEC(ZSTD(3))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192,
    index_granularity_bytes = '10Mi', use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    min_columns_to_activate_adaptive_write_buffer = 500;
CREATE TABLE t_sparse AS t_strings;
CREATE TABLE t_arrays
(
    x Array(UInt32) CODEC(LZ4),
    array32_zstd Array(UInt32) CODEC(ZSTD(3)),
    array64_lz4 Array(UInt64) CODEC(LZ4)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192,
    index_granularity_bytes = '10Mi', use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    propagate_types_serialization_versions_to_nested_types = 1, min_columns_to_activate_adaptive_write_buffer = 500;

INSERT INTO t_strings SELECT x, x, x, x, x FROM (SELECT concat('region_', toString(number % 16)) AS x FROM numbers(100000));
INSERT INTO t_unique SELECT x, x, x, x, x FROM (SELECT toString(cityHash64(number)) AS x FROM numbers(100000));
INSERT INTO t_ints SELECT x, x, x, x, x, x FROM (SELECT number * 3 + number % 7 AS x FROM numbers(100000));
INSERT INTO t_nullable SELECT x, x, x, x FROM (SELECT if(number % 10 = 0, NULL, concat('region_', toString(number % 16))) AS x FROM numbers(100000));
INSERT INTO t_ip SELECT x, x, x, x FROM (SELECT toIPv4(167772160 + number * 7) AS x FROM numbers(100000));
INSERT INTO t_sparse SELECT x, x, x, x, x FROM (SELECT if(number % 50 = 0, concat('x', toString(number)), '') AS x FROM numbers(100000));
-- Rows of about 200 bytes: granules of 8192 rows take more than a state holds at once.
INSERT INTO t_wide SELECT x, x, x, x, x FROM (SELECT concat(repeat('x', 180), toString(cityHash64(number % 1000))) AS x FROM numbers(100000));
INSERT INTO t_arrays SELECT x, x, x FROM (SELECT arrayMap(i -> toUInt32(i * 3 + number % 5), range(number % 20)) AS x FROM numbers(100000));

-- One part per table, and the source column of `t_sparse` is read sparse.
SELECT table, count() FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY table;
SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND active AND table = 't_sparse' AND column = 'x';

CREATE TABLE est (tbl String, col String, r Float64) ENGINE = Memory;
INSERT INTO est SELECT 't_strings', 'str_zstd', estimateCompressionRatio('ZSTD(3)', 'String')(x) FROM t_strings;
INSERT INTO est SELECT 't_strings', 'str_lz4', estimateCompressionRatio('LZ4', 'String')(x) FROM t_strings;
INSERT INTO est SELECT 't_strings', 'lc_lz4', estimateCompressionRatio('LZ4', 'LowCardinality(String)')(x) FROM t_strings;
INSERT INTO est SELECT 't_strings', 'lc_zstd', estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(x) FROM t_strings;
INSERT INTO est SELECT 't_unique', 'str_zstd', estimateCompressionRatio('ZSTD(3)', 'String')(x) FROM t_unique;
INSERT INTO est SELECT 't_unique', 'str_lz4', estimateCompressionRatio('LZ4', 'String')(x) FROM t_unique;
INSERT INTO est SELECT 't_unique', 'lc_lz4', estimateCompressionRatio('LZ4', 'LowCardinality(String)')(x) FROM t_unique;
INSERT INTO est SELECT 't_unique', 'lc_zstd', estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(x) FROM t_unique;
INSERT INTO est SELECT 't_ints', 'u64_lz4', estimateCompressionRatio('LZ4', 'UInt64')(x) FROM t_ints;
INSERT INTO est SELECT 't_ints', 'u64_t64', estimateCompressionRatio('T64, LZ4', 'UInt64')(x) FROM t_ints;
INSERT INTO est SELECT 't_ints', 'u32_t64', estimateCompressionRatio('T64, LZ4', 'UInt32')(x) FROM t_ints;
INSERT INTO est SELECT 't_ints', 'u32_delta', estimateCompressionRatio('Delta, LZ4', 'UInt32')(x) FROM t_ints;
INSERT INTO est SELECT 't_ints', 'u32_doubledelta', estimateCompressionRatio('DoubleDelta, ZSTD(3)', 'UInt32')(x) FROM t_ints;
INSERT INTO est SELECT 't_nullable', 'nullable_zstd', estimateCompressionRatio('ZSTD(3)', 'Nullable(String)')(x) FROM t_nullable;
INSERT INTO est SELECT 't_nullable', 'nullable_lz4', estimateCompressionRatio('LZ4', 'Nullable(String)')(x) FROM t_nullable;
INSERT INTO est SELECT 't_nullable', 'lc_nullable_zstd', estimateCompressionRatio('ZSTD(3)', 'LowCardinality(Nullable(String))')(x) FROM t_nullable;
INSERT INTO est SELECT 't_ip', 'ipv4_lz4', estimateCompressionRatio('LZ4', 'IPv4')(x) FROM t_ip;
INSERT INTO est SELECT 't_ip', 'ipv6_lz4', estimateCompressionRatio('LZ4', 'IPv6')(x) FROM t_ip;
INSERT INTO est SELECT 't_ip', 'ipv6_zstd', estimateCompressionRatio('ZSTD(3)', 'IPv6')(x) FROM t_ip;
INSERT INTO est SELECT 't_sparse', 'str_zstd', estimateCompressionRatio('ZSTD(3)', 'String')(x) FROM t_sparse;
INSERT INTO est SELECT 't_sparse', 'str_lz4', estimateCompressionRatio('LZ4', 'String')(x) FROM t_sparse;
INSERT INTO est SELECT 't_sparse', 'lc_lz4', estimateCompressionRatio('LZ4', 'LowCardinality(String)')(x) FROM t_sparse;
INSERT INTO est SELECT 't_sparse', 'lc_zstd', estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(x) FROM t_sparse;
INSERT INTO est SELECT 't_wide', 'str_zstd', estimateCompressionRatio('ZSTD(3)', 'String')(x) FROM t_wide;
INSERT INTO est SELECT 't_wide', 'str_lz4', estimateCompressionRatio('LZ4', 'String')(x) FROM t_wide;
INSERT INTO est SELECT 't_wide', 'lc_lz4', estimateCompressionRatio('LZ4', 'LowCardinality(String)')(x) FROM t_wide;
INSERT INTO est SELECT 't_wide', 'lc_zstd', estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(x) FROM t_wide;
INSERT INTO est SELECT 't_arrays', 'array32_zstd', estimateCompressionRatio('ZSTD(3)', 'Array(UInt32)')(x) FROM t_arrays;
INSERT INTO est SELECT 't_arrays', 'array64_lz4', estimateCompressionRatio('LZ4', 'Array(UInt64)')(x) FROM t_arrays;

-- The candidates from the smallest to the largest by the estimate, whether the parts give the same order, and whether every
-- estimated size is the stored one times the same factor within 2% (the factor is the bytes of the rows in their own type).
SELECT
    tbl,
    arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((-r, col)))), ' ') AS by_estimate,
    by_estimate = arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((toFloat64(actual), col)))), ' ') AS same_order,
    max(r * actual) / min(r * actual) < 1.02 AS proportional
FROM est
INNER JOIN
(
    SELECT table, column, sum(column_data_compressed_bytes) AS actual
    FROM system.parts_columns WHERE database = currentDatabase() AND active
    GROUP BY table, column
) AS parts ON parts.table = est.tbl AND parts.column = est.col
GROUP BY tbl
ORDER BY tbl;

-- Rows of 1.5 to 3 KB: the part ends its granules at `index_granularity_bytes` (10 MiB) before 8192 rows, and the estimate
-- writes each granule in pieces. The estimated size (the second number of the state, a varint) against the part, within 1%.
CREATE TABLE t_wide_granules (x String CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream', min_columns_to_activate_adaptive_write_buffer = 500;
INSERT INTO t_wide_granules SELECT repeat(toString(number % 89), 1500 + number % 1500) FROM numbers(12000);
SELECT marks > 3 FROM system.parts WHERE database = currentDatabase() AND table = 't_wide_granules' AND active;
SELECT abs(estimated / stored - 1) < 0.01 FROM
(
    SELECT
        (SELECT arraySum(arrayMap((c, i) -> bitShiftLeft(toUInt64(bitAnd(reinterpretAsUInt8(c), 127)), 7 * i), splitByString('', b), range(length(b))))
         FROM (SELECT unhex(extract(hex(estimateCompressionRatioState('ZSTD(1)', 'String')(x)), '^(?:[89A-F][0-9A-F])*[0-7][0-9A-F]((?:[89A-F][0-9A-F])*[0-7][0-9A-F])$')) AS b
               FROM t_wide_granules)) AS estimated,
        (SELECT sum(column_data_compressed_bytes) FROM system.parts_columns
         WHERE database = currentDatabase() AND table = 't_wide_granules' AND active) AS stored
);
DROP TABLE t_wide_granules;

-- A conversion that narrows the rows: `Array(UInt64)` of 2040 elements estimated as `Array(UInt8)`, 2048 bytes a row. The part
-- sizes its granules on the rows it stores, exactly 10 MiB here (5120 rows, an insert of two granules), not on the source rows
-- (643), and the ends of granules end compressed blocks. The estimated size of every codec against the part, within 1%.
CREATE TABLE t_narrowed_zstd1 (x Array(UInt8) CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    min_columns_to_activate_adaptive_write_buffer = 500;
CREATE TABLE t_narrowed_zstd6 (x Array(UInt8) CODEC(ZSTD(6))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    min_columns_to_activate_adaptive_write_buffer = 500;
CREATE TABLE t_narrowed_lz4 (x Array(UInt8) CODEC(LZ4)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    use_const_adaptive_granularity = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    ratio_of_defaults_for_sparse_serialization = 0.9375, compute_exact_num_defaults_for_sparse_columns = 1, serialization_info_version = 'with_types',
    min_columns_to_activate_adaptive_write_buffer = 500;
INSERT INTO t_narrowed_zstd1 SELECT arrayMap(i -> toUInt8((i + number) % 256), range(2040)) FROM numbers(10240);
INSERT INTO t_narrowed_zstd6 SELECT arrayMap(i -> toUInt8((i + number) % 256), range(2040)) FROM numbers(10240);
INSERT INTO t_narrowed_lz4 SELECT arrayMap(i -> toUInt8((i + number) % 256), range(2040)) FROM numbers(10240);
SELECT table, marks FROM system.parts WHERE database = currentDatabase() AND table LIKE 't_narrowed_%' AND active ORDER BY table;
SELECT
    abs(estimated.zstd1 / stored.zstd1 - 1) < 0.01,
    abs(estimated.zstd6 / stored.zstd6 - 1) < 0.01,
    abs(estimated.lz4 / stored.lz4 - 1) < 0.01
FROM
(
    SELECT
        sum(8 + 8 * length(x)) / estimateCompressionRatio('ZSTD(1)', 'Array(UInt8)')(x) AS zstd1,
        sum(8 + 8 * length(x)) / estimateCompressionRatio('ZSTD(6)', 'Array(UInt8)')(x) AS zstd6,
        sum(8 + 8 * length(x)) / estimateCompressionRatio('LZ4', 'Array(UInt8)')(x) AS lz4
    FROM (SELECT arrayMap(i -> toUInt64((i + number) % 256), range(2040)) AS x FROM numbers(10240))
) AS estimated,
(
    SELECT
        sumIf(column_data_compressed_bytes, table = 't_narrowed_zstd1') AS zstd1,
        sumIf(column_data_compressed_bytes, table = 't_narrowed_zstd6') AS zstd6,
        sumIf(column_data_compressed_bytes, table = 't_narrowed_lz4') AS lz4
    FROM system.parts_columns WHERE database = currentDatabase() AND active
) AS stored
SETTINGS max_block_size = 1000;
DROP TABLE t_narrowed_zstd1;
DROP TABLE t_narrowed_zstd6;
DROP TABLE t_narrowed_lz4;

DROP TABLE t_strings;
DROP TABLE t_unique;
DROP TABLE t_ints;
DROP TABLE t_nullable;
DROP TABLE t_ip;
DROP TABLE t_sparse;
DROP TABLE t_arrays;
DROP TABLE t_wide;
DROP TABLE est;
