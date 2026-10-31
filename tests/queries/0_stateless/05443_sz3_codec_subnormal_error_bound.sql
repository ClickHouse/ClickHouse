-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- A positive SZ3 error bound below the smallest normal double, given directly or derived from subnormal data, is
-- compressed losslessly. `ALGO_LORENZO_REG` used to divide it to zero in its regression predictor
-- (`Assertion 'eb != 0' failed` in debug builds).

SET enable_sz3_codec = 1;

DROP TABLE IF EXISTS tab_sz3_subnormal_bound;
CREATE TABLE tab_sz3_subnormal_bound
(
    key UInt64,
    abs64 Float64 CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 1e-322)),
    abs32 Float32 CODEC(SZ3('ALGO_LORENZO_REG', 'ABS', 1e-322)),
    rel64 Float64 CODEC(SZ3('ALGO_LORENZO_REG', 'REL', 0.01)),
    psnr64 Float64 CODEC(SZ3('ALGO_LORENZO_REG', 'PSNR', 0.001))
) ENGINE = MergeTree ORDER BY key;

INSERT INTO tab_sz3_subnormal_bound SELECT number, number, number, number * 5e-324, number % 2 * 5e-324 FROM numbers(1000);
INSERT INTO tab_sz3_subnormal_bound SELECT number, number, number, number * 5e-324, number % 2 * 5e-324 FROM numbers(1000, 1000);
OPTIMIZE TABLE tab_sz3_subnormal_bound FINAL;

SELECT count(), countIf(abs64 = key), countIf(abs32 = toFloat32(key)), countIf(rel64 = key * 5e-324), countIf(psnr64 = key % 2 * 5e-324)
FROM tab_sz3_subnormal_bound;

DROP TABLE tab_sz3_subnormal_bound;
