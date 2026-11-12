-- Tags: no-fasttest, no-parallel, no-parallel-replicas
-- no-fasttest: the fast test build has no vector similarity indexes, because it is built without the `usearch` library
-- no-parallel: other tests clear the whole vector similarity index cache, which would turn the cache hits expected below into misses
-- no-parallel-replicas: the test counts the cache lookups of each search, and with parallel replicas a search can look up its
--     cache entry more than once (also when automatic parallel replicas only collect statistics: the plan built for costing
--     looks it up again)

-- Tests `SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE <table>`: it evicts the cache entries of that table, and keeps
-- the entries of other tables. It requires the privilege on that table only.

DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;
DROP TABLE IF EXISTS tab_outdated;
DROP TABLE IF EXISTS tab_memory;
DROP USER IF EXISTS user_05508;

SELECT '-- Formatting';

-- `DROP` and `CLEAR` are synonyms, and the query is formatted with `CLEAR`.
SELECT formatQuery('SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE tab');
SELECT formatQuery('SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE db.tab');
SELECT formatQuery('SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON CLUSTER c FOR TABLE db.tab');
SELECT formatQuery('SYSTEM CLEAR VECTOR SIMILARITY INDEX CACHE ON CLUSTER c');

-- The table is kept in the JSON representation of the query.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON CLUSTER c FOR TABLE db.tab'));

-- `FOR TABLE` needs a table name.
SELECT formatQuery('SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE'); -- { serverError SYNTAX_ERROR }

-- Now we create tables and run queries

-- `tab1` has two vector similarity indexes, so that the test shows that `FOR TABLE` evicts the entries of all indexes of the
-- table. `tab2` has one.
-- Each table has a single part, and each vector similarity index has a single index granule in it (its default `GRANULARITY`
-- is 100000000), so each search below looks up exactly one cache entry: a hit if the entry is cached, and a miss otherwise.
CREATE TABLE tab1
(
    id Int32,
    vec1 Array(Float32),
    vec2 Array(Float32),
    INDEX idx1 vec1 TYPE vector_similarity('hnsw', 'L2Distance', 2),
    INDEX idx2 vec2 TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab2
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab1 VALUES
    (0, [1.0, 0.0], [0.0, 1.0]),
    (1, [1.1, 0.0], [0.0, 1.1]),
    (2, [1.2, 0.0], [0.0, 1.2]),
    (3, [0.0, 2.0], [2.0, 0.0]),
    (4, [0.0, 2.1], [2.1, 0.0]),
    (5, [0.0, 2.2], [2.2, 0.0]);

INSERT INTO tab2 VALUES
    (0, [1.0, 0.0]),
    (1, [1.1, 0.0]),
    (2, [1.2, 0.0]),
    (3, [0.0, 2.0]),
    (4, [0.0, 2.1]),
    (5, [0.0, 2.2]);

-- Each search sets `log_comment` to the step and the index it searches, so that its cache hits and misses can be found in
-- `system.query_log` at the end of this section.

-- Start with an empty cache: every search misses, and caches its entry.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;
SELECT id FROM tab1 ORDER BY L2Distance(vec1, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'empty cache: tab1.idx1';
SELECT id FROM tab1 ORDER BY L2Distance(vec2, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'empty cache: tab1.idx2';
SELECT id FROM tab2 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'empty cache: tab2.idx';

-- Evicts the entry of `tab2` only.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE tab2;
SELECT id FROM tab1 ORDER BY L2Distance(vec1, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after FOR TABLE tab2: tab1.idx1';
SELECT id FROM tab1 ORDER BY L2Distance(vec2, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after FOR TABLE tab2: tab1.idx2';
SELECT id FROM tab2 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after FOR TABLE tab2: tab2.idx';

-- The table can be qualified with its database. Evicts both entries of `tab1`.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE {CLICKHOUSE_DATABASE:Identifier}.tab1;
SELECT id FROM tab1 ORDER BY L2Distance(vec1, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after FOR TABLE db.tab1: tab1.idx1';
SELECT id FROM tab1 ORDER BY L2Distance(vec2, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after FOR TABLE db.tab1: tab1.idx2';
SELECT id FROM tab2 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after FOR TABLE db.tab1: tab2.idx';

-- Without a table, the whole cache is cleared, as before.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;
SELECT id FROM tab1 ORDER BY L2Distance(vec1, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after no table: tab1.idx1';
SELECT id FROM tab1 ORDER BY L2Distance(vec2, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after no table: tab1.idx2';
SELECT id FROM tab2 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null
    SETTINGS log_comment = 'after no table: tab2.idx';

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

SELECT '-- Outdated parts';

-- A merge leaves the old part `Outdated`, and its cache entries stay until the cleanup thread clears them. Disable the cleanup
-- with a long interval, so that the entry of the outdated part is still there when `FOR TABLE` evicts it.
CREATE TABLE tab_outdated
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS merge_tree_clear_old_parts_interval_seconds = 3600;

INSERT INTO tab_outdated VALUES
    (0, [1.0, 0.0]),
    (1, [1.1, 0.0]),
    (2, [1.2, 0.0]),
    (3, [0.0, 2.0]),
    (4, [0.0, 2.1]),
    (5, [0.0, 2.2]);

-- 4 entries: the 3 of `tab1` and `tab2` from the last step above, and the new one of `tab_outdated`.
SELECT id FROM tab_outdated ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 3 FORMAT Null;
SELECT 'cells after search', value FROM system.metrics WHERE metric = 'VectorSimilarityIndexCacheCells';

-- The merge replaces the part `all_1_1_0` with `all_1_1_1`, which is not cached yet. `all_1_1_0` is `Outdated` now, and its
-- entry is still cached.
OPTIMIZE TABLE tab_outdated FINAL;
SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 'tab_outdated' ORDER BY name;
SELECT 'cells after merge', value FROM system.metrics WHERE metric = 'VectorSimilarityIndexCacheCells';

-- Evicts the entry of the outdated part, and keeps the 3 entries of `tab1` and `tab2`.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE tab_outdated;
SELECT 'cells after FOR TABLE tab_outdated', value FROM system.metrics WHERE metric = 'VectorSimilarityIndexCacheCells';

SELECT '-- Errors';

-- The table must exist.
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE does_not_exist; -- { serverError UNKNOWN_TABLE }

-- Should be a no-op for non-`MergeTree` tables.
CREATE TABLE tab_memory (id Int32) ENGINE = Memory;
SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE tab_memory;

SELECT '-- Access';

-- With `FOR TABLE`, the statement requires the privilege on that table, so the privilege can be granted on a table.
CREATE USER user_05508;
GRANT SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON tab1 TO user_05508;
EXECUTE AS user_05508 SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE tab1;
EXECUTE AS user_05508 SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE tab2; -- { serverError ACCESS_DENIED }

-- The access is checked before the table is looked up, so that the user can't learn whether a table exists.
EXECUTE AS user_05508 SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE does_not_exist; -- { serverError ACCESS_DENIED }

-- Without a table, the statement clears the entries of all tables, so it requires the privilege on all tables.
EXECUTE AS user_05508 SYSTEM DROP VECTOR SIMILARITY INDEX CACHE; -- { serverError ACCESS_DENIED }
GRANT SYSTEM DROP VECTOR SIMILARITY INDEX CACHE ON *.* TO user_05508;
EXECUTE AS user_05508 SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;

DROP USER user_05508;
DROP TABLE tab1;
DROP TABLE tab2;
DROP TABLE tab_outdated;
DROP TABLE tab_memory;
