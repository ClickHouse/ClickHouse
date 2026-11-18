DROP TABLE IF EXISTS squality_in_json_zero_bloom;
CREATE TABLE squality_in_json_zero_bloom
(
    j JSON,
    INDEX idx JSONAllPaths(j) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;
INSERT INTO squality_in_json_zero_bloom VALUES ('{"other":1}'), ('{"a":0}');

SELECT count() FROM squality_in_json_zero_bloom
WHERE j.a::Float64 IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 0;
SELECT count() FROM squality_in_json_zero_bloom
WHERE j.a::Float64 IN (toFloat64(-0.))
SETTINGS in_null_as_unknown = 1, use_skip_indexes = 1;
DROP TABLE squality_in_json_zero_bloom;
