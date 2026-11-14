-- Regression test: the sampled-failure pruning bound of the `Wallaby` scale chooser must not count
-- an integer sample against a negative scale it lands near. Integers that are bit-exact at scale 0
-- report no lower near scale, yet here they quantize at scale -16 to 1 and 2 with an adjustment of
-- one ULP; the pair of exact 1e16 values discovers that scale, and pruning it after the sample
-- growth fell back to XOR.
-- https://github.com/ClickHouse/ClickHouse/pull/113575

SET enable_wallaby_codec = 1;

DROP TABLE IF EXISTS negative_scale_base;
DROP TABLE IF EXISTS negative_scale_wallaby;

CREATE TABLE negative_scale_base (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE negative_scale_wallaby (i UInt32 CODEC(NONE), f Float64 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;

INSERT INTO negative_scale_base SELECT number,
    multiIf(number % 1024 < 2, toFloat64(10000000000000000), intDiv(number % 1024, 2) % 2 = 0, toFloat64(10000000000000002), toFloat64(20000000000000004))
    FROM numbers(102400);
INSERT INTO negative_scale_wallaby SELECT i, f FROM negative_scale_base;
OPTIMIZE TABLE negative_scale_wallaby FINAL;

SELECT '# Roundtrip is lossless';
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM negative_scale_base AS b INNER JOIN negative_scale_wallaby AS a USING i;

SELECT '# The negative scale is not pruned: much smaller than the XOR encoding';
SELECT (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'negative_scale_wallaby' AND name = 'f') < 100 * 800;

DROP TABLE negative_scale_base;
DROP TABLE negative_scale_wallaby;
