-- The codec combinator `FirstApplicable` uses the first alternative applicable to the data type of every stream.

DROP TABLE IF EXISTS t_first_applicable;
DROP TABLE IF EXISTS t_first_applicable_default;
DROP TABLE IF EXISTS t_first_applicable_ttl;
DROP TABLE IF EXISTS t_first_applicable_error;

SELECT '-- column codecs';

CREATE TABLE t_first_applicable
(
    i UInt32 CODEC(FirstApplicable(FPC, Multiple(T64, ZSTD(3)), ZSTD(3))),
    f Float64 CODEC(FirstApplicable(FPC, Multiple(T64, ZSTD(3)), ZSTD(3))),
    s String CODEC(FirstApplicable(FPC, Multiple(T64, ZSTD(3)), ZSTD(3))),
    t Tuple(a Float32, b UInt16, c String) CODEC(FirstApplicable(FPC, Multiple(T64, ZSTD(3)), ZSTD(3))),
    a Array(Int64) CODEC(FirstApplicable(FPC, Multiple(T64, ZSTD(3)), ZSTD(3))),
    n Nullable(Float32) CODEC(firstapplicable(FPC, ZSTD(3))),
    d UInt64 CODEC(Delta, FirstApplicable(FPC, LZ4)),
    -- No alternative applies without a data type, so the sizes of the array get no generic codec, as with `CODEC(T64)`.
    e Array(Int64) CODEC(FirstApplicable(FPC, T64))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0;

SHOW CREATE TABLE t_first_applicable;

INSERT INTO t_first_applicable
SELECT number, number / 7, toString(number), (number / 3, number % 1000, toString(number % 10)), range(number % 5), if(number % 3 = 0, NULL, number / 11), number * 3, range(number % 3)
FROM numbers(10000);

SELECT count(), sum(i), sum(f), sum(length(s)), sum(t.a), sum(t.b), sum(length(t.c)), sum(length(a)), sum(n), sum(d), sum(arraySum(e)) FROM t_first_applicable;

SELECT column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_first_applicable)
ORDER BY column, substream;

SELECT '-- the codec follows a change of the data type';

ALTER TABLE t_first_applicable MODIFY COLUMN i Float64 SETTINGS mutations_sync = 2;

SELECT count(), sum(i) FROM t_first_applicable;

SELECT column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_first_applicable)
WHERE column = 'i';

SELECT '-- default_compression_codec';

CREATE TABLE t_first_applicable_default
(
    i UInt32,
    f Float64,
    s String,
    d UInt64 CODEC(Delta, Default)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0,
    default_compression_codec = 'FirstApplicable(FPC, Multiple(T64, LZ4), ZSTD(1))',
    marks_compression_codec = 'FirstApplicable(FPC, ZSTD(1))',
    primary_key_compression_codec = 'FirstApplicable(T64, LZ4)';

INSERT INTO t_first_applicable_default SELECT number, number / 7, toString(number), number * 3 FROM numbers(10000);
INSERT INTO t_first_applicable_default SELECT number, number / 7, toString(number), number * 3 FROM numbers(10000);
OPTIMIZE TABLE t_first_applicable_default FINAL;

SELECT count(), sum(i), sum(f), sum(length(s)), sum(d) FROM t_first_applicable_default;

SELECT column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_first_applicable_default)
ORDER BY column, substream;

SELECT '-- TTL RECOMPRESS';

CREATE TABLE t_first_applicable_ttl
(
    i UInt32,
    s String
)
ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime(i) RECOMPRESS CODEC(FirstApplicable(T64, ZSTD(1)))
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0, default_compression_codec = 'LZ4';

INSERT INTO t_first_applicable_ttl SELECT number, toString(number) FROM numbers(10000);
OPTIMIZE TABLE t_first_applicable_ttl FINAL;

SELECT count(), sum(i), sum(length(s)) FROM t_first_applicable_ttl;

SELECT column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_first_applicable_ttl)
ORDER BY column, substream;

SELECT '-- estimateCompressionRatio';

SELECT estimateCompressionRatio('FirstApplicable(FPC, ZSTD)')(number) > 1, estimateCompressionRatio('FirstApplicable(FPC, ZSTD)')(toString(number)) > 1 FROM numbers(10000);

SELECT '-- errors';

-- No alternative is applicable to `String`.
CREATE TABLE t_first_applicable_error (s String CODEC(FirstApplicable(FPC, T64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_first_applicable_error (s String CODEC(FirstApplicable())) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_first_applicable_error (s String CODEC(FirstApplicable(ZSTD, Multiple()))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
-- Every alternative is validated, not only the selected one.
CREATE TABLE t_first_applicable_error (f Float64 CODEC(FirstApplicable(FPC, NoSuchCodec))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_CODEC }
CREATE TABLE t_first_applicable_error (f Float64 CODEC(FirstApplicable(FPC, Delta(3)))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_CODEC_PARAMETER }
-- The resolved chain passes the usual checks: no transformation after a generic compression.
CREATE TABLE t_first_applicable_error (i UInt32 CODEC(FirstApplicable(ZSTD, LZ4), Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
-- `Multiple` is allowed only as an alternative of `FirstApplicable`.
CREATE TABLE t_first_applicable_error (i UInt32 CODEC(Multiple(T64, ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_CODEC }
-- A declarative codec takes effect only directly in the column declaration.
SET enable_quantized_codec = 1;
CREATE TABLE t_first_applicable_error (v Array(Float32) CODEC(FirstApplicable(Quantized('int8', 4), ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
-- Without a data type, an alternative applicable to any data type is required.
CREATE TABLE t_first_applicable_error (i UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'FirstApplicable(FPC, T64)'; -- { serverError BAD_ARGUMENTS }
-- `Default` has no meaning in a setting.
CREATE TABLE t_first_applicable_error (i UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'FirstApplicable(FPC, Default)'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_first_applicable;
DROP TABLE t_first_applicable_default;
DROP TABLE t_first_applicable_ttl;
