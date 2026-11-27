-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the two-stage quantized-codes rewrite is deliberately disabled for a parallel-replicas
-- read, so the plan-shape assertion below cannot hold there (the query still returns exact results in that case).

-- The `product` method of the `Quantized(...)` column codec (Product Quantization: a per-part codebook trained with
-- k-means, one m-byte code per vector) estimates both `L2Distance` and `cosineDistance`. With
-- `vector_search_use_quantized_codes = 1` the optimizer rewrites `ORDER BY <distance>(vec, ref) LIMIT k` into a
-- shortlist ranked over the quantized codes plus a rescore against the full-precision vectors, so the returned
-- distances are exact and only the recall depends on the shortlist size (`vector_search_index_fetch_multiplier`).
-- Expected behaviour source: docs/reference/engines/table-engines/mergetree-family/annindexes.mdx, section
-- "Vector search with quantized codecs", and docs/reference/statements/create/table/codec.mdx, section "Quantized".
-- 02354_vector_search_quantize_codec_pq.sql covers the `L2Distance` shortlist; the `cosineDistance` shortlist asserted
-- here ranks the codes with a different lookup table (partial inner products plus the query norm and the centroid
-- norms, instead of partial squared distances), so it ranks by the angle alone.
-- Recall for pseudo-random vectors is not asserted: it is approximate by design, and the codebook is trained from the
-- first block of the part, whose size the test harness randomizes. The queries on `pq_cosine` below therefore
-- oversample the shortlist until it covers the whole table, which makes their asserted result the exact one, as
-- documented. The second fixture `pq_cosine_exact` instead uses a two-valued-per-coordinate vector set, which any
-- codebook encodes losslessly, so a bounded shortlist there is deterministic and tells the two metrics apart.

SET enable_quantized_codec = 1;              -- the codec is experimental and gated behind this setting
SET vector_search_use_quantized_codes = 1;   -- the two-stage rewrite is opt-in and off by default
-- The shortlist size is clamped to `query_plan_max_limit_for_lazy_materialization`, which the test harness randomizes
-- down to values as small as 1; pin it so the oversampled shortlists below really do cover every row.
SET query_plan_max_limit_for_lazy_materialization = 1000000;

DROP TABLE IF EXISTS pq_cosine;
CREATE TABLE pq_cosine
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 32, 8, 8))
)
ENGINE = MergeTree ORDER BY id;

-- 1500 deterministic pseudo-random vectors in [-1, 1), plus row 1500, which holds exactly 1024 * the vector of row 123
-- (scaling a Float32 by a power of two is exact). Cosine distance ignores the vector magnitude, so row 1500 sits at
-- cosine distance 0 from row 123's vector, the same as row 123 itself, while it is the single most distant row of the
-- table by `L2Distance`.
INSERT INTO pq_cosine (id, vec)
SELECT
    number,
    arrayMap(j -> toFloat32(if(number = 1500, 1024., 1.) * (sipHash64(if(number = 1500, 123, number), j) % 2000 / 1000.0 - 1.0)), range(32))
FROM numbers(1501);

-- The optimizer engages the two-stage rewrite for `cosineDistance` too, ranking the shortlist with the internal
-- `productQuantizationDistance` over the codes subcolumn and the per-part codebook subcolumn.
SELECT
    'plan',
    countIf(explain ILIKE '%quantized shortlist%') > 0 AS has_shortlist,
    countIf(explain ILIKE '%productQuantizationDistance%') > 0 AS has_pq_distance
FROM
(
    EXPLAIN actions = 1
    SELECT id FROM pq_cosine
    ORDER BY cosineDistance(vec, (SELECT vec FROM pq_cosine WHERE id = 123)) ASC
    LIMIT 5 SETTINGS vector_search_index_fetch_multiplier = 50
);

-- Ranked by the angle, the scaled copy of the reference vector is as near as the exact match: both are at cosine
-- distance 0, and the next row of the table is at 0.4.
WITH (SELECT vec FROM pq_cosine WHERE id = 123) AS ref
SELECT 'cosine_top2_is_scale_invariant',
    (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM pq_cosine ORDER BY cosineDistance(vec, ref) ASC LIMIT 2 SETTINGS vector_search_index_fetch_multiplier = 1000));

-- Ranked by `L2Distance` over the same codes and the same codebook, the scaled copy is instead the most distant row of
-- the table, so it is not part of the result.
WITH (SELECT vec FROM pq_cosine WHERE id = 123) AS ref
SELECT 'l2_top2_excludes_scaled_copy',
    (SELECT has(groupArray(id), 1500) FROM (SELECT id FROM pq_cosine ORDER BY L2Distance(vec, ref) ASC LIMIT 2 SETTINGS vector_search_index_fetch_multiplier = 1000)) = 0;

-- The codes path returns the same top-k as an exact scan: the codes only pick the candidates and the result is
-- rescored against the full-precision vectors. The exact side orders by two keys, which leaves it outside the rewrite
-- (that needs a single distance sort column).
WITH (SELECT vec FROM pq_cosine WHERE id = 123) AS ref
SELECT 'top10_matches_exact_scan',
    (SELECT arraySort(groupArray(id)) FROM (SELECT id, cosineDistance(vec, ref) AS d FROM pq_cosine ORDER BY d, id LIMIT 10))
  = (SELECT arraySort(groupArray(id)) FROM (SELECT id FROM pq_cosine ORDER BY cosineDistance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 1000));

DROP TABLE pq_cosine;

-- A fixture that pins down which metric the shortlist itself ranks by, with a shortlist of a single row (LIMIT 1 at
-- `vector_search_index_fetch_multiplier = 1` makes k' = 1) out of a hundred rows, so the exact rescore cannot repair a
-- wrong candidate. Two interleaved classes of 50 rows each, every coordinate taken from a two-element value set:
--   'parallel' = [1, 0, 0 ...]  -- the direction of the reference vector, at cosine distance 0 but L2 distance 3
--   'near_l2'  = [3, 1, 0 ...]  -- cosine distance 0.0513, but the nearest row by L2 distance (sqrt(2))
-- With one coordinate per subspace (32 dimensions / m = 32) and 256 centroids per subspace drawn from the data, a
-- codebook of any part of this table reproduces both values exactly, so the codes rank exactly as the vectors do.
DROP TABLE IF EXISTS pq_cosine_exact;
CREATE TABLE pq_cosine_exact
(
    id UInt32,
    cls String,
    vec Array(Float32) CODEC(Quantized('product', 32, 8, 8))
)
ENGINE = MergeTree ORDER BY id
-- Both classes must reach the block the codebook is trained from; the test harness randomizes these two down to one
-- row per granule, which would train the codebook on a single row.
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

INSERT INTO pq_cosine_exact
SELECT number, if(number % 2 = 0, 'parallel', 'near_l2'),
    CAST(arrayConcat(if(number % 2 = 0, [1., 0.], [3., 1.]), arrayMap(j -> 0., range(30))), 'Array(Float32)')
FROM numbers(100);

-- The shortlist is ranked by the angle: the single candidate is a 'parallel' row, which is also the exact answer.
WITH (SELECT CAST(arrayConcat([4., 0.], arrayMap(j -> 0., range(30))), 'Array(Float32)')) AS ref
SELECT 'cosine_top1_of_one_candidate', cls, round(cosineDistance(vec, ref), 4)
FROM pq_cosine_exact ORDER BY cosineDistance(vec, ref) ASC LIMIT 1
SETTINGS vector_search_index_fetch_multiplier = 1;

-- The same shortlist ranked by `L2Distance` keeps the other class instead, so the result above cannot be produced by an
-- L2-ranked shortlist.
WITH (SELECT CAST(arrayConcat([4., 0.], arrayMap(j -> 0., range(30))), 'Array(Float32)')) AS ref
SELECT 'l2_top1_of_one_candidate', cls, round(L2Distance(vec, ref), 4)
FROM pq_cosine_exact ORDER BY L2Distance(vec, ref) ASC LIMIT 1
SETTINGS vector_search_index_fetch_multiplier = 1;

DROP TABLE pq_cosine_exact;
