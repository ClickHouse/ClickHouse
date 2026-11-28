DROP TABLE IF EXISTS squality_nested_setting_bloom_zero;
CREATE TABLE squality_nested_setting_bloom_zero
(
    x Float64,
    INDEX x_bloom x TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO squality_nested_setting_bloom_zero VALUES (0), (1), (2);

SELECT count() FROM
    (SELECT * FROM squality_nested_setting_bloom_zero SETTINGS in_null_as_unknown = 0)
WHERE x IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 0;

SELECT count() FROM
    (SELECT * FROM squality_nested_setting_bloom_zero SETTINGS in_null_as_unknown = 0)
WHERE x IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 1;

DROP TABLE squality_nested_setting_bloom_zero;
