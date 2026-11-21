-- Random settings limits: use_lz4_library_decompression=(0, 0)
-- Reads that end exactly at the end of an LZ4 block decompress it straight into the column, which may overwrite
-- only the unused capacity and the right padding of the column. The results must match the uncompressed table it is copied from.

DROP TABLE IF EXISTS t_lz4_exact_block;
DROP TABLE IF EXISTS t_none_exact_block;

CREATE TABLE t_lz4_exact_block
(
    k UInt64, u8 UInt8, s String, ns Nullable(String), fs FixedString(16), d Decimal64(3), uu UUID, ip4 IPv4, ip6 IPv6, a Array(UInt32)
)
ENGINE = MergeTree ORDER BY k
SETTINGS default_compression_codec = 'LZ4', index_granularity = 8192, index_granularity_bytes = 10485760,
    min_compress_block_size = 4096, max_compress_block_size = 1048576, min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types',
    string_serialization_version = 'with_size_stream', propagate_types_serialization_versions_to_nested_types = 1;

CREATE TABLE t_none_exact_block AS t_lz4_exact_block
ENGINE = MergeTree ORDER BY k
SETTINGS default_compression_codec = 'NONE', index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_none_exact_block SELECT
    number, number % 251, repeat('x', number % 61) || toString(number), if(number % 3 = 0, NULL, toString(number % 977)),
    toFixedString(toString(number % 1000000), 16), number / 7, generateUUIDv4(number), toIPv4(number), toIPv6(toIPv4(number)), range(number % 7)
FROM numbers(100000);
INSERT INTO t_lz4_exact_block SELECT * FROM t_none_exact_block;

SET preferred_block_size_bytes = 0;
SET use_uncompressed_cache = 0;

SET max_block_size = 8192;
SELECT 8192, (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_lz4_exact_block) = (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_none_exact_block);
SELECT 8192, (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_lz4_exact_block WHERE k % 3 = 0) = (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_none_exact_block WHERE k % 3 = 0);
SELECT 8192, (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_lz4_exact_block WHERE k BETWEEN 20000 AND 40000) = (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_none_exact_block WHERE k BETWEEN 20000 AND 40000);

SET max_block_size = 1000;
SELECT 1000, (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_lz4_exact_block) = (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_none_exact_block);

SET max_block_size = 65536;
SELECT 65536, (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_lz4_exact_block) = (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_none_exact_block);
SELECT 65536, (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_lz4_exact_block WHERE k % 3 = 0) = (SELECT sum(cityHash64(k, u8, s, isNull(ns), ifNull(ns, ''), fs, d, uu, ip4, ip6, a)) FROM t_none_exact_block WHERE k % 3 = 0);

DROP TABLE t_lz4_exact_block;
DROP TABLE t_none_exact_block;
