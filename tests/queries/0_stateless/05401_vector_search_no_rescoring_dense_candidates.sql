-- Tags: no-fasttest
-- Without rescoring, vector search returns only rows shortlisted by the index (#124230).

SET enable_parallel_replicas = 0;
SET vector_search_with_rescoring = 0;
SET vector_search_filter_strategy = 'postfilter';
SET vector_search_index_fetch_multiplier = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO tab VALUES (0, [0, 0]), (1, [1, 0]), (2, [2, 0]), (3, [3, 0]);

-- The index shortlists ids 0, 1, 2; the filter drops id 0.
SELECT id, L2Distance(vec, [0., 0.]) FROM tab WHERE id != 0 ORDER BY L2Distance(vec, [0., 0.]) LIMIT 3 SETTINGS optimize_move_to_prewhere = 0;
SELECT id FROM tab WHERE id != 0 ORDER BY L2Distance(vec, [0., 0.]) LIMIT 3 SETTINGS vector_search_with_rescoring = 1;

DROP TABLE tab;

CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
INSERT INTO tab VALUES (0, [0, 0]), (1, [2000, 0]), (2, [3000, 0]), (3, [4000, 0]);

-- Distances above 1000, no filter.
SELECT id FROM tab ORDER BY L2Distance(vec, [0., 0.]) LIMIT 3;

DROP TABLE tab;
