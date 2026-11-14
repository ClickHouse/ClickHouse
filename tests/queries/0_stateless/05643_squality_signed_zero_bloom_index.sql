DROP TABLE IF EXISTS squality_signed_zero_bloom;

CREATE TABLE squality_signed_zero_bloom
(
    x Float64,
    y Float32,
    INDEX x_bloom x TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX y_bloom y TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO squality_signed_zero_bloom SELECT toFloat64(0), toFloat32(-1) * toFloat32(0);
INSERT INTO squality_signed_zero_bloom VALUES (1, 1), (2, 2);

SELECT count() FROM squality_signed_zero_bloom WHERE x IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 0;
SELECT count() FROM squality_signed_zero_bloom WHERE x IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 1;
SELECT count() FROM squality_signed_zero_bloom WHERE y IN (toFloat32(0))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 0;
SELECT count() FROM squality_signed_zero_bloom WHERE y IN (toFloat32(0))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 1;

DROP TABLE squality_signed_zero_bloom;
