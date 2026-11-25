-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is disabled under parallel replicas, and the shortlist below depends on it)
-- The shortlist is computed from integer popcounts of the codes, so it is the same on every platform. Code sizes that are
-- not a multiple of 16 bytes: rabitq 136 dims (17 bytes), rabitq 1000 dims (125 bytes), turboquant 1000 dims (two codes).

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_popcount_tail;
CREATE TABLE quantize_popcount_tail
(
    id UInt32,
    r136 Array(Float32) CODEC(Quantized('rabitq', 136)),
    r1000 Array(Float32) CODEC(Quantized('rabitq', 1000)),
    t1000 Array(Float32) CODEC(Quantized('turboquant', 1000))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_popcount_tail
SELECT number,
    arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(136)),
    arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(1000)),
    arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(1000))
FROM numbers(3000);

WITH (SELECT r136 FROM quantize_popcount_tail WHERE id = 42) AS ref
SELECT 'r136', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_popcount_tail ORDER BY cosineDistance(r136, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT r136 FROM quantize_popcount_tail WHERE id = 42) AS ref
SELECT 'r136_300', sum(id) FROM (SELECT id FROM quantize_popcount_tail ORDER BY cosineDistance(r136, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);

WITH (SELECT r1000 FROM quantize_popcount_tail WHERE id = 42) AS ref
SELECT 'r1000', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_popcount_tail ORDER BY cosineDistance(r1000, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT r1000 FROM quantize_popcount_tail WHERE id = 42) AS ref
SELECT 'r1000_300', sum(id) FROM (SELECT id FROM quantize_popcount_tail ORDER BY cosineDistance(r1000, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);

WITH (SELECT t1000 FROM quantize_popcount_tail WHERE id = 42) AS ref
SELECT 't1000', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_popcount_tail ORDER BY cosineDistance(t1000, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT t1000 FROM quantize_popcount_tail WHERE id = 42) AS ref
SELECT 't1000_300', sum(id) FROM (SELECT id FROM quantize_popcount_tail ORDER BY cosineDistance(t1000, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);

DROP TABLE quantize_popcount_tail;
