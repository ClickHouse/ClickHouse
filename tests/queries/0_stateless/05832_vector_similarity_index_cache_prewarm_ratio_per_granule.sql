-- Tags: no-parallel, no-fasttest, no-ordinary-database
-- no-parallel: fills the server-wide vector similarity index cache up to the prewarm ratio

-- One part, several index granules, max_threads = 1. Prewarm must stop once the cache reaches
-- `vector_similarity_index_cache_prewarm_ratio` and must not deserialize the remaining granules.

SET parallel_replicas_local_plan = 1;
SET max_threads = 1;
SET max_insert_threads = 1;

SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;

DROP TABLE IF EXISTS vs_prewarm_probe;
DROP TABLE IF EXISTS vs_prewarm_granules;

CREATE TABLE vs_prewarm_probe
(
    id UInt64,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO vs_prewarm_probe VALUES (0, [0.0, 0.0]);

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_prewarm_probe;

CREATE TABLE vs_prewarm_granules
(
    id UInt64,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- More granules than fit under the ratio, plus a few in case a granule is smaller than the probe.
INSERT INTO vs_prewarm_granules
SELECT number, [toFloat32(number), 0.0]
FROM numbers(
    (
        SELECT assumeNotNull(intDiv(toUInt64(toFloat64(cache_size) * ratio), granule_size) + 3)
        FROM
        (
            SELECT
                (SELECT toUInt64(value) FROM system.server_settings WHERE name = 'vector_similarity_index_cache_size') AS cache_size,
                (SELECT toFloat64(value) FROM system.server_settings WHERE name = 'vector_similarity_index_cache_prewarm_ratio') AS ratio,
                (SELECT max(size) FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_prewarm_probe') AS granule_size
        )
    )
);

SELECT
    (SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table = 'vs_prewarm_granules') = 1
    AND (SELECT count() FROM vs_prewarm_granules) > 1;

SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_prewarm_granules;

-- Cached marks are a contiguous prefix of the part (oldest marks may have been evicted by the
-- insert that crossed the ratio). The last granule of the part must not have been loaded.
SELECT
    cached > 0
    AND max_mark + 1 < granules
    AND cached = max_mark - min_mark + 1
    AND min_mark = max_mark + 1 - cached
FROM
(
    SELECT
        (SELECT count() FROM vs_prewarm_granules) AS granules,
        count() AS cached,
        min(index_mark) AS min_mark,
        max(index_mark) AS max_mark
    FROM system.vector_similarity_index_cache
    WHERE database = currentDatabase() AND table = 'vs_prewarm_granules'
);

DROP TABLE vs_prewarm_probe SYNC;
DROP TABLE vs_prewarm_granules SYNC;

SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;
