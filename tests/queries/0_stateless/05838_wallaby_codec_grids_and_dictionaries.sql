-- The `Wallaby` codec turns floats into integers when they lie on a grid: not only decimals, but also averages
-- over several samples (a rational grid such as 1/6000) and values produced by a multiplication, which are not
-- correctly rounded decimals. It gives dictionary symbols to frequent values whatever they are, and it codes
-- the positions and the values of what does not fit (`NaN`) instead of storing them verbatim.
-- https://github.com/ClickHouse/ClickHouse/pull/113575

SET enable_wallaby_codec = 1;

DROP TABLE IF EXISTS grids_base;
DROP TABLE IF EXISTS grids_wallaby;

CREATE TABLE grids_base (i UInt32, averaged Float32, multiplied Float64, distinct_values Float32, rare Float64)
    ENGINE = MergeTree ORDER BY i
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

CREATE TABLE grids_wallaby
(
    i UInt32 CODEC(NONE),
    averaged Float32 CODEC(Wallaby),
    multiplied Float64 CODEC(Wallaby),
    distinct_values Float32 CODEC(Wallaby),
    rare Float64 CODEC(Wallaby)
)
    ENGINE = MergeTree ORDER BY i
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1,
        index_granularity = 8192, index_granularity_bytes = 10485760, min_compress_block_size = 65536, max_compress_block_size = 1048576;

INSERT INTO grids_base SELECT
    number,
    toFloat32((cityHash64(number, 1) % 60000) / 6000),
    (cityHash64(number, 2) % 100000) * 0.01,
    toFloat32(sqrt(cityHash64(number, 3) % 1000 + 2)),
    multiIf(cityHash64(number, 4) % 100 = 0, nan, cityHash64(number, 4) % 50 = 0, (cityHash64(number, 5) % 1000) / 10, 0)
FROM numbers(100000);

INSERT INTO grids_wallaby SELECT * FROM grids_base;
OPTIMIZE TABLE grids_wallaby FINAL;

SELECT '# Roundtrip is lossless';
SELECT count(), sum(bin(a.averaged) <> bin(b.averaged)), sum(bin(a.multiplied) <> bin(b.multiplied)),
    sum(bin(a.distinct_values) <> bin(b.distinct_values)), sum(bin(a.rare) <> bin(b.rare))
FROM grids_base AS b INNER JOIN grids_wallaby AS a USING i;

SELECT '# Averages over 60 samples, a grid of 1/6000: 15.9 bits of information per value';
SELECT sum(data_compressed_bytes) * 8 / 100000 < 17 FROM system.columns WHERE database = currentDatabase() AND table = 'grids_wallaby' AND name = 'averaged';

SELECT '# Decimals produced by a multiplication: 16.6 bits of information per value';
SELECT sum(data_compressed_bytes) * 8 / 100000 < 17.5 FROM system.columns WHERE database = currentDatabase() AND table = 'grids_wallaby' AND name = 'multiplied';

SELECT '# A thousand distinct values that are not decimals: 10 bits of information per value plus the dictionaries';
SELECT sum(data_compressed_bytes) * 8 / 100000 < 14 FROM system.columns WHERE database = currentDatabase() AND table = 'grids_wallaby' AND name = 'distinct_values';

SELECT '# Rare values and NaN among zeros';
SELECT sum(data_compressed_bytes) * 8 / 100000 < 0.7 FROM system.columns WHERE database = currentDatabase() AND table = 'grids_wallaby' AND name = 'rare';

DROP TABLE grids_base;
DROP TABLE grids_wallaby;
