-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is disabled under parallel replicas, and the shortlist below depends on it)
-- The codes scan of 'int8' and 'prefix' scores several rows per step; the shortlist must not depend on how rows are
-- grouped. 3001 rows (not a multiple of 4), the reference vector of the last row, 6 kernels (3 codecs x 2 metrics).

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS quantize_flat_rows;
CREATE TABLE quantize_flat_rows
(
    id UInt32,
    i8 Array(Float32) CODEC(Quantized('int8', 100)),
    pb Array(Float32) CODEC(Quantized('prefix', 100, 37, 'bf16')),
    pi Array(Float32) CODEC(Quantized('prefix', 100, 37, 'int8'))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO quantize_flat_rows
SELECT id, v, v, v
FROM (SELECT number AS id, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(100)) AS v FROM numbers(3001));

WITH (SELECT i8 FROM quantize_flat_rows WHERE id = 3000) AS ref
SELECT 'i8_cos', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_flat_rows ORDER BY cosineDistance(i8, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT i8 FROM quantize_flat_rows WHERE id = 42) AS ref
SELECT 'i8_cos_300', sum(id) FROM (SELECT id FROM quantize_flat_rows ORDER BY cosineDistance(i8, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT i8 FROM quantize_flat_rows WHERE id = 3000) AS ref
SELECT 'i8_l2', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_flat_rows ORDER BY L2Distance(i8, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT i8 FROM quantize_flat_rows WHERE id = 42) AS ref
SELECT 'i8_l2_300', sum(id) FROM (SELECT id FROM quantize_flat_rows ORDER BY L2Distance(i8, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);

WITH (SELECT pb FROM quantize_flat_rows WHERE id = 3000) AS ref
SELECT 'pb_cos', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_flat_rows ORDER BY cosineDistance(pb, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT pb FROM quantize_flat_rows WHERE id = 42) AS ref
SELECT 'pb_cos_300', sum(id) FROM (SELECT id FROM quantize_flat_rows ORDER BY cosineDistance(pb, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT pb FROM quantize_flat_rows WHERE id = 3000) AS ref
SELECT 'pb_l2', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_flat_rows ORDER BY L2Distance(pb, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT pb FROM quantize_flat_rows WHERE id = 42) AS ref
SELECT 'pb_l2_300', sum(id) FROM (SELECT id FROM quantize_flat_rows ORDER BY L2Distance(pb, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);

WITH (SELECT pi FROM quantize_flat_rows WHERE id = 3000) AS ref
SELECT 'pi_cos', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_flat_rows ORDER BY cosineDistance(pi, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT pi FROM quantize_flat_rows WHERE id = 42) AS ref
SELECT 'pi_cos_300', sum(id) FROM (SELECT id FROM quantize_flat_rows ORDER BY cosineDistance(pi, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT pi FROM quantize_flat_rows WHERE id = 3000) AS ref
SELECT 'pi_l2', arraySort(groupArray(id)) FROM (SELECT id FROM quantize_flat_rows ORDER BY L2Distance(pi, ref) LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1);
WITH (SELECT pi FROM quantize_flat_rows WHERE id = 42) AS ref
SELECT 'pi_l2_300', sum(id) FROM (SELECT id FROM quantize_flat_rows ORDER BY L2Distance(pi, ref) LIMIT 300 SETTINGS vector_search_index_fetch_multiplier = 1);

DROP TABLE quantize_flat_rows;
