-- Tags: no-fasttest

-- A vector search must not size its candidate buffers by a LIMIT (or hnsw_candidate_list_size_for_search) far
-- above the number of vectors in the index: on a 10-vector index both values below reserved 2 GiB.

DROP TABLE IF EXISTS tab_limit;
DROP TABLE IF EXISTS tab_ef;

CREATE TABLE tab_limit (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
CREATE TABLE tab_ef AS tab_limit;
INSERT INTO tab_limit VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);
INSERT INTO tab_ef SELECT * FROM tab_limit;

WITH [0.0, 2.0] AS reference_vec
SELECT id FROM tab_limit ORDER BY L2Distance(vec, reference_vec) LIMIT 134217728
SETTINGS max_limit_for_vector_search_queries = 134217728, log_comment = '05896_vector_search_limit_above_index_size_limit';

WITH [0.0, 2.0] AS reference_vec
SELECT id FROM tab_ef ORDER BY L2Distance(vec, reference_vec) LIMIT 3
SETTINGS hnsw_candidate_list_size_for_search = 134217728, log_comment = '05896_vector_search_limit_above_index_size_ef';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, sum(ProfileEvents['USearchSearchCount']) > 0, max(memory_usage) < 1000000000
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05896_vector_search_limit_above_index_size_limit', '05896_vector_search_limit_above_index_size_ef')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE tab_limit;
DROP TABLE tab_ef;
