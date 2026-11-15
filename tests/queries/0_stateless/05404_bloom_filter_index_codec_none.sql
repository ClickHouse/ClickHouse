-- The blocks of a `bloom_filter` index that the part's codec does not compress well are written with codec `NONE`:
-- the filter bits are usually incompressible, so a generic codec only slows down reading the index.
-- Compressible blocks and other indices keep the part's codec.

DROP TABLE IF EXISTS t_bf_codec;

-- In a multi-column `bloom_filter` index, the filter of each column is sized by the largest number of distinct
-- values among the index columns, so the filter of the constant column `b` is almost all zeros and compresses
-- well with `ZSTD`. Such an index keeps the part's codec.
-- Every granule has the same 1000 values of `c`, so the filters of `bf_repeated` are identical and compress well.
-- The `tokenbf_v1` filter over a single repeated token is compressible as well.
CREATE TABLE t_bf_codec
(
    a UInt64,
    b UInt64,
    c UInt64,
    s String,
    INDEX bf a TYPE bloom_filter GRANULARITY 1,
    INDEX bf_multi (a, b) TYPE bloom_filter GRANULARITY 1,
    INDEX bf_repeated c TYPE bloom_filter GRANULARITY 1,
    INDEX tbf s TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a
SETTINGS default_compression_codec = 'ZSTD(1)', index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t_bf_codec SELECT number, 0, number % 1000, 'a' FROM numbers(100000);

SELECT name, data_compressed_bytes >= data_uncompressed_bytes AS uncompressed
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_bf_codec'
ORDER BY name;

-- Merges write the index the same way.
INSERT INTO t_bf_codec SELECT number + 100000, 0, number % 1000, 'a' FROM numbers(100000);
OPTIMIZE TABLE t_bf_codec FINAL;

SELECT name, data_compressed_bytes >= data_uncompressed_bytes AS uncompressed
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_bf_codec'
ORDER BY name;

SELECT count() FROM t_bf_codec WHERE a = 150000 SETTINGS force_data_skipping_indices = 'bf';
SELECT count() FROM t_bf_codec WHERE b = 1 SETTINGS force_data_skipping_indices = 'bf_multi';
SELECT count() FROM t_bf_codec WHERE c = 1000 SETTINGS force_data_skipping_indices = 'bf_repeated';

CHECK TABLE t_bf_codec SETTINGS check_query_single_value_result = 1;

DROP TABLE t_bf_codec;
