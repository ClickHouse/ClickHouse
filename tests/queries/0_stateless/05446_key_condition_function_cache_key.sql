-- A key column whose name looks like an internal name, and several functions over one key column, must not change the result.

DROP TABLE IF EXISTS t_key_function_cache;
CREATE TABLE t_key_function_cache (`_0_1` UInt8, a UInt64) ENGINE = MergeTree ORDER BY (`_0_1`, a) SETTINGS index_granularity = 128;
INSERT INTO t_key_function_cache SELECT 0, number FROM numbers(50000);

SELECT count() FROM t_key_function_cache WHERE intDiv(a, 50) = 200
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1, use_query_condition_cache = 0;
SELECT count() FROM t_key_function_cache WHERE intDiv(a, 50) = 200
SETTINGS use_lightweight_primary_key_index_analysis = 1, force_primary_key = 1, use_query_condition_cache = 0;

SELECT count() FROM t_key_function_cache WHERE intDiv(a, 49) = 204 AND intDiv(a, 50) IN (200, 201)
SETTINGS use_lightweight_primary_key_index_analysis = 0, force_primary_key = 1, use_query_condition_cache = 0;
SELECT count() FROM t_key_function_cache WHERE intDiv(a, 49) = 204 AND intDiv(a, 50) IN (200, 201)
SETTINGS use_lightweight_primary_key_index_analysis = 1, force_primary_key = 1, use_query_condition_cache = 0;

DROP TABLE t_key_function_cache;
