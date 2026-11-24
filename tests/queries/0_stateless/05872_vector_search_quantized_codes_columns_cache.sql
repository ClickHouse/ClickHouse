-- Tags: no-parallel, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- no-parallel: drops the global columns cache. no-parallel-replicas: the two-stage rewrite is disabled there.

-- With `vector_search_cache_quantized_codes`, the two-stage vector search reads the quantized codes through the
-- columns cache even if `use_columns_cache` is disabled, but never caches the full-precision vectors.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;
SET use_columns_cache = 0;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

DROP TABLE IF EXISTS t_codes_cache;
CREATE TABLE t_codes_cache
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_codes_cache SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(4000);

SELECT '-- codes only, from disk and then from the cache';
SYSTEM DROP COLUMNS CACHE;
WITH (SELECT vec FROM t_codes_cache WHERE id = 123) AS ref
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref) LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000))
    = (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref), id LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0));
SELECT arraySort(groupUniqArray(column)) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_codes_cache';
WITH (SELECT vec FROM t_codes_cache WHERE id = 123) AS ref
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref) LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000))
    = (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref), id LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0));
SELECT arraySort(groupUniqArray(column)) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_codes_cache';

SELECT '-- a filter column is read and cached';
SYSTEM DROP COLUMNS CACHE;
WITH (SELECT vec FROM t_codes_cache WHERE id = 123) AS ref
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache WHERE id % 10 != 3 ORDER BY cosineDistance(vec, ref) LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000))
    = (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache WHERE id % 10 != 3 ORDER BY cosineDistance(vec, ref), id LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0));
SELECT arraySort(groupUniqArray(column)) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_codes_cache';
WITH (SELECT vec FROM t_codes_cache WHERE id = 123) AS ref
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache WHERE id % 10 != 3 ORDER BY cosineDistance(vec, ref) LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000))
    = (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache WHERE id % 10 != 3 ORDER BY cosineDistance(vec, ref), id LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0));

SELECT '-- setting disabled';
SYSTEM DROP COLUMNS CACHE;
WITH (SELECT vec FROM t_codes_cache WHERE id = 123) AS ref
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref) LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000, vector_search_cache_quantized_codes = 0))
    = (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref), id LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0));
SELECT arraySort(groupUniqArray(column)) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_codes_cache';

SELECT '-- lazy materialization disabled: the read reads the vectors, so it does not use the cache';
SYSTEM DROP COLUMNS CACHE;
WITH (SELECT vec FROM t_codes_cache WHERE id = 123) AS ref
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref) LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000, query_plan_optimize_lazy_materialization = 0))
    = (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache ORDER BY cosineDistance(vec, ref), id LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0));
SELECT arraySort(groupUniqArray(column)) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_codes_cache';

SELECT '-- a filter on the vector column: the vectors are never cached';
SYSTEM DROP COLUMNS CACHE;
WITH (SELECT vec FROM t_codes_cache WHERE id = 123) AS ref
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache WHERE vec[1] > -2 ORDER BY cosineDistance(vec, ref) LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 1000))
    = (SELECT groupArray(id) FROM (SELECT id FROM t_codes_cache WHERE vec[1] > -2 ORDER BY cosineDistance(vec, ref), id LIMIT 5 SETTINGS vector_search_use_quantized_codes = 0));
SELECT countIf(column = 'vec') FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_codes_cache';

SYSTEM DROP COLUMNS CACHE;
DROP TABLE t_codes_cache;
