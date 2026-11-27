-- Columns with a chain of codecs are compressed with the `Multiple` codec.

SET allow_suspicious_codecs = 1;

DROP TABLE IF EXISTS t_codec_multiple;

CREATE TABLE t_codec_multiple
(
    a UInt64 CODEC(Delta, LZ4),
    b UInt64 CODEC(DoubleDelta, T64, ZSTD(1)),
    c String CODEC(LZ4, ZSTD(1)),
    d Float64 CODEC(Gorilla, LZ4HC(1), NONE)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_codec_multiple SELECT number, number * 3, toString(number % 1000), number / 7 FROM numbers(100000);

SELECT count(), sum(a), sum(b), uniqExact(c), round(sum(d), 3) FROM t_codec_multiple;

OPTIMIZE TABLE t_codec_multiple FINAL;

SELECT count(), sum(a), sum(b), uniqExact(c), round(sum(d), 3) FROM t_codec_multiple;

DROP TABLE t_codec_multiple;
