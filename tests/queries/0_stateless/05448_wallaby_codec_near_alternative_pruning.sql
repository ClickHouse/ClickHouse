-- Regression test: the sampled-failure pruning bound of the `Wallaby` scale chooser must not count
-- a sample that voted an exact scale as a failure of its lower near scale. Here every sample votes
-- a fine exact scale with a coarse near alternative, and the near scale (one value bit plus narrow
-- adjustments) is the cheapest encoding; pruning it after the sample growth fell back to XOR.
-- https://github.com/ClickHouse/ClickHouse/pull/113575

SET enable_wallaby_codec = 1;

DROP TABLE IF EXISTS near_base;
DROP TABLE IF EXISTS near_wallaby32;
DROP TABLE IF EXISTS near_wallaby64;

CREATE TABLE near_base (i UInt32 CODEC(NONE), f64 Float64 CODEC(NONE), f32 Float32 CODEC(NONE)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE near_wallaby64 (i UInt32 CODEC(NONE), f Float64 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;
CREATE TABLE near_wallaby32 (i UInt32 CODEC(NONE), f Float32 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;

INSERT INTO near_base SELECT number, 1000000.00400001 + (number % 2) * 0.001, if(number % 2 = 0, toFloat32(8192.01), toFloat32(8193.01)) FROM numbers(102400);
INSERT INTO near_wallaby64 SELECT i, f64 FROM near_base;
INSERT INTO near_wallaby32 SELECT i, f32 FROM near_base;
OPTIMIZE TABLE near_wallaby64 FINAL;
OPTIMIZE TABLE near_wallaby32 FINAL;

SELECT '# Roundtrip is lossless';
SELECT count(), sum(bin(a.f) <> bin(b.f64)) FROM near_base AS b INNER JOIN near_wallaby64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f32)) FROM near_base AS b INNER JOIN near_wallaby32 AS a USING i;

SELECT '# The near scale is not pruned: smaller than the XOR encoding';
SELECT (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'near_wallaby64' AND name = 'f') < 100 * 1300;
SELECT (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'near_wallaby32' AND name = 'f') < 100 * 840;

DROP TABLE near_base;
DROP TABLE near_wallaby32;
DROP TABLE near_wallaby64;
