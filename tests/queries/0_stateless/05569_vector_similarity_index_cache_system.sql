-- Tags: no-parallel, no-fasttest, no-ordinary-database
-- no-parallel: prewarm fills a server-wide cache. The test clears it, which
-- would race with other vector-search tests, and CI's 128 MiB cache is too
-- small to share. Vector similarity indexes need usearch.

SET parallel_replicas_local_plan = 1;

SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;

DROP TABLE IF EXISTS vs_cache_t1;
DROP TABLE IF EXISTS vs_cache_t2;
DROP TABLE IF EXISTS vs_cache_two_idx;
DROP TABLE IF EXISTS vs_cache_memory;
DROP TABLE IF EXISTS vs_cache_plain;

CREATE TABLE vs_cache_t1
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE vs_cache_t2
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO vs_cache_t1 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]), (3, [0.0, 2.1]);
INSERT INTO vs_cache_t2 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]), (3, [0.0, 2.1]);

SELECT 'after insert', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table IN ('vs_cache_t1', 'vs_cache_t2');

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_t1;
SELECT 'after prewarm t1', count() > 0 FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_t1';

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_t2;
SELECT 'after prewarm t1 and t2', count() > 1 FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table IN ('vs_cache_t1', 'vs_cache_t2');

CREATE TABLE vs_cache_two_idx
(
    id Int32,
    vec_a Array(Float32),
    vec_b Array(Float32),
    INDEX idx_a vec_a TYPE vector_similarity('hnsw', 'L2Distance', 2),
    INDEX idx_b vec_b TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO vs_cache_two_idx VALUES (0, [1.0, 0.0], [0.0, 1.0]), (1, [1.1, 0.0], [0.0, 1.1]);

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_two_idx INDEX idx_a;
SELECT 'prewarm idx_a', arraySort(groupUniqArray(index_name)) FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_two_idx';

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_two_idx;
SELECT 'prewarm both indexes', arraySort(groupUniqArray(index_name)) FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_two_idx';

SELECT 'prewarm keys before ann', count() AS cached FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_t1';

SELECT count()
FROM
(
    WITH [0.0, 2.0] AS reference_vec
    SELECT id
    FROM vs_cache_t1
    ORDER BY L2Distance(vec, reference_vec)
    LIMIT 1
);

SELECT 'prewarm keys after ann', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_t1';

CREATE TABLE vs_cache_memory (id Int32, vec Array(Float32)) ENGINE = Memory;
CREATE TABLE vs_cache_plain (id Int32) ENGINE = MergeTree ORDER BY id;

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_memory; -- { serverError BAD_ARGUMENTS }
SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_plain; -- { serverError BAD_ARGUMENTS }
SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE; -- { clientError SYNTAX_ERROR }
SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_t1 INDEX missing; -- { serverError BAD_ARGUMENTS }

DROP TABLE vs_cache_t1 SYNC;
DROP TABLE vs_cache_t2 SYNC;
DROP TABLE vs_cache_two_idx SYNC;
DROP TABLE vs_cache_memory;
DROP TABLE vs_cache_plain;
