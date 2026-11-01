-- Tags: no-fasttest
-- no-fasttest: the PCO codec is implemented in Rust, which is not built in fasttest.

-- `FirstApplicable(PCO, ZSTD(3))`: PCO for numbers, ZSTD for everything else, including as the default codec of a table,
-- where PCO cannot be given alone because the data type is not known there.

DROP TABLE IF EXISTS t_first_applicable_pco;

SET enable_pco_codec = 1;

CREATE TABLE t_first_applicable_pco
(
    i UInt64,
    f Float64,
    s String,
    a Array(Int32),
    c UInt64 CODEC(FirstApplicable(PCO(4), ZSTD(1))),
    cs String CODEC(FirstApplicable(PCO(4), ZSTD(1)))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_adaptive_codec_selection = 0, default_compression_codec = 'FirstApplicable(PCO, ZSTD(3))';

INSERT INTO t_first_applicable_pco SELECT number, number / 7, toString(number), range(number % 5), number * 3, toString(number % 100) FROM numbers(10000);

SELECT count(), sum(i), sum(f), sum(length(s)), sum(length(a)), sum(arraySum(a)), sum(c), sum(length(cs)) FROM t_first_applicable_pco;

SELECT column, substream, mapKeys(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), t_first_applicable_pco)
ORDER BY column, substream;

DROP TABLE t_first_applicable_pco;

-- An alternative is gated by its setting even if it would not be selected.
SET enable_pco_codec = 0;
CREATE TABLE t_first_applicable_pco (s String CODEC(FirstApplicable(PCO, ZSTD))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_first_applicable_pco (s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS default_compression_codec = 'FirstApplicable(PCO, ZSTD)'; -- { serverError BAD_ARGUMENTS }
