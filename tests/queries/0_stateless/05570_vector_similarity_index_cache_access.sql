-- Tags: no-parallel, no-fasttest, no-ordinary-database
-- no-parallel: creates a global user with a fixed name, and clears the server-wide
-- vector similarity index cache so a previous test's delayed drop cannot fill it.
-- Vector similarity indexes need usearch. Ordinary databases have no table UUID.

SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;

DROP USER IF EXISTS u_05570_vs_cache;
DROP TABLE IF EXISTS vs_cache_secret;
DROP TABLE IF EXISTS vs_cache_grant_t;

CREATE TABLE vs_cache_secret
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO vs_cache_secret VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]), (3, [0.0, 2.1]);
SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_secret;

CREATE USER u_05570_vs_cache IDENTIFIED WITH no_password;
GRANT SELECT ON system.vector_similarity_index_cache TO u_05570_vs_cache;

SELECT 'without show', count() FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_secret';
EXECUTE AS u_05570_vs_cache SELECT 'hidden', count() FROM system.vector_similarity_index_cache WHERE table = 'vs_cache_secret';

GRANT SHOW TABLES, SHOW COLUMNS ON vs_cache_secret TO u_05570_vs_cache;
EXECUTE AS u_05570_vs_cache SELECT 'visible', count() > 0 FROM system.vector_similarity_index_cache WHERE table = 'vs_cache_secret';

CREATE TABLE vs_cache_grant_t
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO vs_cache_grant_t VALUES (0, [1.0, 0.0]), (1, [0.0, 1.0]);

GRANT SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE ON vs_cache_grant_t TO u_05570_vs_cache;

EXECUTE AS u_05570_vs_cache SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_secret; -- { serverError ACCESS_DENIED }
EXECUTE AS u_05570_vs_cache SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE vs_cache_grant_t;
SELECT 'after table-level prewarm', count() > 0 FROM system.vector_similarity_index_cache WHERE database = currentDatabase() AND table = 'vs_cache_grant_t';

DROP TABLE vs_cache_secret SYNC;
DROP TABLE vs_cache_grant_t SYNC;
DROP USER u_05570_vs_cache;
