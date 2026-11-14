-- `RECOMPRESS COLUMN` must detect a column codec that references `Default` through a codec
-- combinator: `FirstApplicable` resolves a nested `Default` against the table's default codec too.
-- `T64` is not applicable to `String`, so `Default` is selected, and after a change of
-- `default_compression_codec` such a column must go through the whole-part rewrite instead of
-- recompressing its blocks with the part's stored default codec.

SET allow_experimental_recompress_column = 1;
SET mutations_sync = 2;
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_recompress_first_applicable_default;

CREATE TABLE t_recompress_first_applicable_default
(
    k UInt64,
    s String CODEC(FirstApplicable(T64, Default)),
    n String CODEC(FirstApplicable(T64, FirstApplicable(T64, Default)))
)
ENGINE = MergeTree ORDER BY k
SETTINGS default_compression_codec = 'NONE',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0,
    enable_adaptive_codec_selection = 0;

INSERT INTO t_recompress_first_applicable_default SELECT number, repeat('a', 100), repeat('b', 100) FROM numbers(10000);

SELECT DISTINCT 'part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_first_applicable_default' AND active;
SELECT 'none is large', column, sum(data_compressed_bytes) > 1000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_first_applicable_default' AND column IN ('s', 'n') AND active
GROUP BY column ORDER BY column;

ALTER TABLE t_recompress_first_applicable_default MODIFY SETTING default_compression_codec = 'ZSTD';
ALTER TABLE t_recompress_first_applicable_default RECOMPRESS COLUMN s;
ALTER TABLE t_recompress_first_applicable_default RECOMPRESS COLUMN n;

SELECT 'after', count(), countIf(s = repeat('a', 100)), countIf(n = repeat('b', 100)) FROM t_recompress_first_applicable_default;
SELECT 'zstd is small', column, sum(data_compressed_bytes) < 100000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_first_applicable_default' AND column IN ('s', 'n') AND active
GROUP BY column ORDER BY column;

CHECK TABLE t_recompress_first_applicable_default;

DROP TABLE t_recompress_first_applicable_default;
