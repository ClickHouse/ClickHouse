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
DROP TABLE IF EXISTS tab_readded;
DROP TABLE IF EXISTS tab_renamed;
DROP TABLE IF EXISTS tab_prefetch;
DROP TABLE IF EXISTS tab_conversion;

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

-- A column dropped by a pending mutation and added again is still in the part, but it is computed from its default
-- expression as well, so the vectors are read as usual then.
CREATE TABLE tab_readded
(
    id UInt32,
    d UInt64,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;

SYSTEM STOP MERGES tab_readded;
INSERT INTO tab_readded SELECT id, 999, vec FROM tab;
ALTER TABLE tab_readded DROP COLUMN d SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE tab_readded ADD COLUMN d UInt64 DEFAULT intDiv(64, length(vec)) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'added again, setting on';
WITH (SELECT vec FROM tab_readded WHERE id = 123) AS ref
SELECT id, d FROM tab_readded ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = '05618_readded_on';

SELECT 'added again, setting off';
WITH (SELECT vec FROM tab_readded WHERE id = 123) AS ref
SELECT id, d FROM tab_readded ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, query_plan_optimize_lazy_materialization = 0;

-- A column with a default expression renamed by a pending mutation has its old name in the part. It is computed from
-- its default expression as well, so the vectors are read as usual then.
CREATE TABLE tab_renamed
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;

SYSTEM STOP MERGES tab_renamed;
INSERT INTO tab_renamed SELECT id, vec FROM tab;
ALTER TABLE tab_renamed ADD COLUMN d UInt64 DEFAULT intDiv(64, length(vec));
ALTER TABLE tab_renamed RENAME COLUMN d TO e SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'renamed, setting on';
WITH (SELECT vec FROM tab_renamed WHERE id = 123) AS ref
SELECT id, e FROM tab_renamed ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = '05618_renamed_on';

SELECT 'renamed, setting off';
WITH (SELECT vec FROM tab_renamed WHERE id = 123) AS ref
SELECT id, e FROM tab_renamed ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, query_plan_optimize_lazy_materialization = 0;

-- A default expression that does not use the vector column does not matter: the vectors are read only for the candidates.
ALTER TABLE tab ADD COLUMN f UInt64 DEFAULT id * 2;

SELECT 'other default, setting on';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id, f FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = '05618_other_default_on';

SELECT 'other default, setting off';
WITH (SELECT vec FROM tab WHERE id = 123) AS ref
SELECT id, f FROM tab ORDER BY cosineDistance(vec, ref) LIMIT 10
SETTINGS merge_tree_use_one_row_per_block_layout = 0, query_plan_optimize_lazy_materialization = 0;

-- With few prefetches allowed, a reader prefetched for one task is reused for the next tasks, and must read past the end
-- of the range of the first task.
CREATE TABLE tab_prefetch
(
    id UInt32,
    vec Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;

INSERT INTO tab_prefetch SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(20000);

SELECT 'reused reader';
WITH (SELECT vec FROM tab_prefetch WHERE id = 123) AS ref
SELECT id FROM tab_prefetch ORDER BY cosineDistance(vec, ref) LIMIT 3
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, max_threads = 2,
    allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_prefetch = 1, local_filesystem_read_method = 'pread_threadpool',
    filesystem_prefetch_step_marks = 1, filesystem_prefetches_limit = 2, merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 0, use_reader_executor = 1;

-- A column with a pending conversion to another type is converted before the candidates are filtered, and the
-- conversion would fail on the placeholder values, so it is read as usual.
CREATE TABLE tab_conversion
(
    id UInt32,
    vec Array(Float32) CODEC(NONE),
    payload Array(Float32) CODEC(NONE) SETTINGS (min_compress_block_size = 256, max_compress_block_size = 256),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 64, 'b1', 64, 128)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 128;

SYSTEM STOP MERGES tab_conversion;
INSERT INTO tab_conversion SELECT id, vec, arrayMap(j -> toFloat32(id + j), range(64)) FROM tab;
ALTER TABLE tab_conversion MODIFY COLUMN payload QBit(Float32, 64) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'pending conversion, setting on';
WITH (SELECT vec FROM tab_conversion WHERE id = 123) AS ref
SELECT id, toString(payload) FROM tab_conversion ORDER BY cosineDistance(vec, ref) LIMIT 3
SETTINGS merge_tree_use_one_row_per_block_layout = 1, query_plan_optimize_lazy_materialization = 0, log_comment = '05618_conversion_on';

SELECT 'pending conversion, setting off';
WITH (SELECT vec FROM tab_conversion WHERE id = 123) AS ref
SELECT id, toString(payload) FROM tab_conversion ORDER BY cosineDistance(vec, ref) LIMIT 3
SETTINGS merge_tree_use_one_row_per_block_layout = 0, query_plan_optimize_lazy_materialization = 0;

SYSTEM FLUSH LOGS query_log;

-- Only the candidates of the index are read this way, and none with the setting off.
SELECT log_comment, ProfileEvents['MergeTreeOneRowPerBlockReadRows'] > 0 AS read_by_blocks
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05618_on', '05618_off', '05618_default_on', '05618_readded_on', '05618_renamed_on', '05618_other_default_on', '05618_conversion_on')
ORDER BY log_comment;

DROP TABLE tab;
DROP TABLE tab_readded;
DROP TABLE tab_renamed;
DROP TABLE tab_prefetch;
DROP TABLE tab_conversion;
