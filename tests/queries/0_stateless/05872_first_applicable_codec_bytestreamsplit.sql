-- `FirstApplicable(Multiple(ByteStreamSplit, LZ4), ZSTD)`: `ByteStreamSplit` for the fixed-size types
-- with an element of at least 2 bytes, `ZSTD` for everything else, including as the default codec of a table.

DROP TABLE IF EXISTS t_first_applicable_bss;

SET enable_bytestreamsplit_codec = 1;

CREATE TABLE t_first_applicable_bss
(
    i UInt64,
    f Float32,
    u Int128,
    b UInt8,
    s String,
    a Array(Int32),
    c Float64 CODEC(FirstApplicable(Multiple(ByteStreamSplit, LZ4), ZSTD(1))),
    cs String CODEC(FirstApplicable(Multiple(ByteStreamSplit, LZ4), ZSTD(1))),
    cb Int8 CODEC(FirstApplicable(ByteStreamSplit, ZSTD(1)))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0, ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream', nullable_serialization_version = 'basic', default_compression_codec = 'FirstApplicable(Multiple(ByteStreamSplit, LZ4), ZSTD(3))';

INSERT INTO t_first_applicable_bss SELECT number, number / 7, number * 1000000007, number % 200, toString(number), range(number % 5), number / 3, toString(number % 100), number % 100 FROM numbers(10000);

SELECT count(), sum(i), round(sum(f), 1), sum(u), sum(b), sum(length(s)), sum(length(a)), sum(arraySum(a)), round(sum(c), 3), sum(length(cs)), sum(cb) FROM t_first_applicable_bss;

SELECT column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_first_applicable_bss)
ORDER BY column, substream;

DROP TABLE t_first_applicable_bss;
