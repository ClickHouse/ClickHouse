-- A text index reader that a read task hands a rebuilt granule of its part must not keep postings streams of the old granule.
-- The split of the read into layers counts the marks of a layer border twice, so the cached index read result of the big
-- part is released before its last read task, which rebuilds it. Detected by sanitizer builds (heap-use-after-free).

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, text String, INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'pfor') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi', text_index_posting_list_block_size = 16;

INSERT INTO tab SELECT number, concat('abc def row ', toString(number)) FROM numbers(6400);
OPTIMIZE TABLE tab FINAL;
SYSTEM STOP MERGES tab;
INSERT INTO tab SELECT 3000 + number, concat('abc def row ', toString(number)) FROM numbers(300);
INSERT INTO tab SELECT 3100 + number, concat('abc def row ', toString(number)) FROM numbers(300);

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_streams_to_max_threads_ratio = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1;
SET use_query_condition_cache = 0, optimize_move_to_prewhere = 0;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1;
SET max_rows_to_read = 0; -- A throwing row limit applies the skip indexes before the read instead of on data read.
SET query_plan_optimize_count_from_text_index = 0;
SET merge_tree_min_rows_for_concurrent_read = 64, merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1;
SET merge_tree_min_bytes_per_read_stream = 0, allow_prefetched_read_pool_for_local_filesystem = 0, read_backoff_min_latency_ms = 0;

SELECT count() FROM tab WHERE hasAllTokens(text, ['abc', 'def']) SETTINGS text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab WHERE hasAllTokens(text, ['abc', 'def']) SETTINGS text_index_posting_list_apply_mode = 'lazy';

DROP TABLE tab;
