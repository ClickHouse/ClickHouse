DROP TABLE IF EXISTS squality_map_keys_zero_in;
CREATE TABLE squality_map_keys_zero_in
(
    m Map(String, Float64),
    INDEX bf mapKeys(m) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO squality_map_keys_zero_in VALUES ({'other': 1}), ({'missing': 0});

SELECT count() FROM squality_map_keys_zero_in
WHERE m['missing'] IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 0;
SELECT count() FROM squality_map_keys_zero_in
WHERE m['missing'] IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 1;
DROP TABLE squality_map_keys_zero_in;

DROP TABLE IF EXISTS squality_map_values_zero_in;
CREATE TABLE squality_map_values_zero_in
(
    m Map(String, Float64),
    INDEX bf mapValues(m) TYPE bloom_filter(0.0001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO squality_map_values_zero_in VALUES ({'k': 0}), ({'k': 1}), ({'other': 2});

SELECT count() FROM squality_map_values_zero_in
WHERE m['k'] IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 0;
SELECT count() FROM squality_map_values_zero_in
WHERE m['k'] IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 1;
DROP TABLE squality_map_values_zero_in;
