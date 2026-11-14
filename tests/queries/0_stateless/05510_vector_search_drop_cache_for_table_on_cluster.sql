-- Tags: no-fasttest, no-parallel, no-parallel-replicas
-- no-fasttest: the fast test build has no vector similarity indexes, because it is built without the `usearch` library
-- no-parallel: other tests clear the whole vector similarity index cache, which would turn the cache hits expected below into
--     misses. Also, the test creates a table in the `default` database, and its name is the same in every run.
-- no-parallel-replicas: the test counts the cache lookups of each search, and with parallel replicas a search can look up its
--     cache entry more than once

-- Tests `SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON CLUSTER c FOR TABLE <table>` with a bare table name, run from a current
-- database other than `default`. The initiator checks the access to the table in its current database. The workers run the
-- queued query without the initiator's current database and would resolve a bare name in `default`, so the initiator must
-- qualify the name in the queued query. Otherwise the command evicts the entries of `default.<table>`, which the access check
-- did not cover, and keeps the entries of the table it was meant for.

SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS tab_05510;
DROP TABLE IF EXISTS default.tab_05510;
DROP USER IF EXISTS user_05510;

-- Two tables with the same name, one in the current database and one in `default`. Each has a single part with a single index
-- granule in it, so each search below looks up exactly one cache entry: a hit if the entry is cached, and a miss otherwise.
CREATE TABLE tab_05510
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE default.tab_05510
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_05510 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [0.0, 2.0]), (4, [0.0, 2.1]), (5, [0.0, 2.2]);
INSERT INTO default.tab_05510 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [0.0, 2.0]), (4, [0.0, 2.1]), (5, [0.0, 2.2]);

-- The user may drop the cache entries of the table in the current database only.
CREATE USER user_05510;
GRANT CLUSTER ON *.* TO user_05510;
GRANT SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON tab_05510 TO user_05510;

-- Start with an empty cache: both searches miss, and cache their entries.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;
SELECT id FROM tab_05510 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'empty cache: current database';
SELECT id FROM default.tab_05510 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'empty cache: default';

-- The bare name means the table in the current database, which the user has the privilege on. Evicts its entry only.
EXECUTE AS user_05510 SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON CLUSTER test_shard_localhost FOR TABLE tab_05510;
SELECT id FROM tab_05510 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after bare name: current database';
SELECT id FROM default.tab_05510 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after bare name: default';

-- The user has no privilege on the table in `default`.
EXECUTE AS user_05510 SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON CLUSTER test_shard_localhost FOR TABLE default.tab_05510; -- { serverError ACCESS_DENIED }

-- An explicit database is kept in the queued query. Evicts the entry of the table in `default` only.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON CLUSTER test_shard_localhost FOR TABLE default.tab_05510;
SELECT id FROM tab_05510 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after default.tab_05510: current database';
SELECT id FROM default.tab_05510 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after default.tab_05510: default';

SELECT '-- Cache hits and misses (left is hits, right is misses)';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['VectorSimilarityIndexCacheHits'] AS hits,
    ProfileEvents['VectorSimilarityIndexCacheMisses'] AS misses
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%ORDER BY L2Distance%'
ORDER BY event_time_microseconds;

DROP USER user_05510;
DROP TABLE tab_05510;
DROP TABLE default.tab_05510;
