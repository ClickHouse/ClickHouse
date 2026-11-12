-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
-- no-fasttest: the vector similarity index is not in the fast test build.
-- no-parallel-replicas: the vector search optimization, and with it the candidate rows, is disabled under parallel replicas.

-- With `merge_tree_use_one_row_per_block_layout` and `vector_search_with_rescoring`, only the candidate rows of the vector
-- similarity index are read from the vector column, not whole granules. This needs one row per block, stored without
-- compression. The results must be the same as with the usual readers.

SET vector_search_with_rescoring = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
-- The setting has no effect with the columns cache or the uncompressed cache.
SET use_columns_cache = 0;
SET use_uncompressed_cache = 0;
-- Enough candidates that rescoring always finds the exact nearest neighbours, whatever the approximation of the index.
SET vector_search_index_fetch_multiplier = 100;
SET hnsw_candidate_list_size_for_search = 2048;

DROP TABLE IF EXISTS tab;

-- 64 `Float32` elements are 256 bytes, so every row is one block.
CREATE TABLE tab
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;

INSERT INTO tab SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(2000);

SELECT 'setting on';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, log_comment = '05618_on';

SELECT 'brute force';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS use_skip_indexes = 0;

SELECT 'setting off';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, log_comment = '05618_off';

-- A column added later is computed from its default expression before the candidates are filtered. The expression could
-- use the placeholder values of the other rows, so the vectors are read as usual then.
ALTER TABLE tab ADD COLUMN d UInt64 DEFAULT intDiv(64, length(vec));

SELECT 'default from the vector, setting on';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id, d FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = '05618_default_on';

SELECT 'default from the vector, setting off';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id, d FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, query_plan_optimize_lazy_materialization = 0;

SYSTEM FLUSH LOGS query_log;

-- Only the candidates of the index are read this way, and none with the setting off.
SELECT log_comment, ProfileEvents['MergeTreeOneRowPerBlockReadRows'] > 0 AS read_by_blocks
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05618_on', '05618_off', '05618_default_on')
ORDER BY log_comment;

DROP TABLE tab;
