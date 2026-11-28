SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_in_signed_zero_fixed;
CREATE TABLE squality_in_signed_zero_fixed (x Float64, y UInt64)
ENGINE = MergeTree ORDER BY (bitShiftRight(reinterpretAsUInt64(x), 63), y);
INSERT INTO squality_in_signed_zero_fixed SELECT toFloat64(0), 2
UNION ALL SELECT toFloat64(-0.), 1;

SELECT y FROM squality_in_signed_zero_fixed WHERE x IN (toFloat64(0.))
ORDER BY y LIMIT 1
SETTINGS optimize_read_in_order = 0, use_primary_key = 0, use_skip_indexes = 0,
    optimize_use_implicit_projections = 0, max_threads = 1;
SELECT y FROM squality_in_signed_zero_fixed WHERE x IN (toFloat64(0.))
ORDER BY y LIMIT 1
SETTINGS optimize_read_in_order = 1, use_primary_key = 0, use_skip_indexes = 0,
    optimize_use_implicit_projections = 0, max_threads = 1;

DROP TABLE squality_in_signed_zero_fixed;
