SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_in_float_zero_key_transform;
CREATE TABLE squality_in_float_zero_key_transform (x Float64)
ENGINE = MergeTree ORDER BY toString(x)
SETTINGS index_granularity = 1;
INSERT INTO squality_in_float_zero_key_transform VALUES (0), (1), (2);

SELECT count() FROM squality_in_float_zero_key_transform
WHERE x IN (toFloat64(-0.))
SETTINGS use_primary_key = 0, use_skip_indexes = 0, optimize_use_implicit_projections = 0;

SELECT count() FROM squality_in_float_zero_key_transform
WHERE x IN (toFloat64(-0.))
SETTINGS use_primary_key = 1, use_skip_indexes = 0, optimize_use_implicit_projections = 0;

DROP TABLE squality_in_float_zero_key_transform;
