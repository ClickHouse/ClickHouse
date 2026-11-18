-- Tags: no-parallel, no-fasttest, no-ordinary-database

-- Prewarm must not evict granules already cached for another table, even when
-- the second table has more parts than fit under the 0.95 ratio of the 128 MiB
-- CI cache (one 2-dim HNSW granule is about 17 MiB).

SET max_threads = 4;

SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;

DROP TABLE IF EXISTS vs_cache_keep;
DROP TABLE IF EXISTS vs_cache_fill;

CREATE TABLE vs_cache_keep
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE vs_cache_fill
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192, parts_to_delay_insert = 100, parts_to_throw_insert = 100;

SYSTEM STOP MERGES vs_cache_keep;
SYSTEM STOP MERGES vs_cache_fill;

INSERT INTO vs_cache_keep VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]), (3, [0.0, 2.1]);
SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_keep;
SELECT 'keep after prewarm', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_keep';

INSERT INTO vs_cache_fill VALUES (0, [1.0, 0.0]), (1, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (2, [1.0, 0.0]), (3, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (4, [1.0, 0.0]), (5, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (6, [1.0, 0.0]), (7, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (8, [1.0, 0.0]), (9, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (10, [1.0, 0.0]), (11, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (12, [1.0, 0.0]), (13, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (14, [1.0, 0.0]), (15, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (16, [1.0, 0.0]), (17, [0.0, 1.0]);
INSERT INTO vs_cache_fill VALUES (18, [1.0, 0.0]), (19, [0.0, 1.0]);

SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_fill;
SELECT 'keep survived', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_keep';
SELECT 'fill did not take every part', count() < 10 FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_fill';

DROP TABLE vs_cache_keep SYNC;
DROP TABLE vs_cache_fill SYNC;
