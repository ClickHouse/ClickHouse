-- Tags: no-parallel-replicas
-- `ReadFromTextIndexCount` counts the parts of an index with a posting list codec in slices of 10'000'000 rows
-- claimed by all streams, so a large part is not counted by a single stream. The counts and the read rows must match
-- the normal read path.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
SET text_index_posting_list_apply_mode = 'lazy';
SET max_rows_to_group_by = 0;
-- The granules pruned by the query condition cache entries of the previous queries disable the optimization.
SET use_query_condition_cache = 0;
-- `max_rows_to_read` with `read_overflow_mode = 'throw'` disables `use_skip_indexes_on_data_read`.
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfor', posting_list_block_size = 1000) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- The large part has two slices: `edge` is in the first rows of both, `tail` only in the second one,
-- `needle` and `pin` are in a few granules of the first and the second slice.
INSERT INTO tab SELECT number, concat(
    'every',
    if(number % 2 = 0, ' half', ''),
    if(number % 3 = 0, ' third', ''),
    if(number % 7 = 0, ' seventh', ''),
    if(number % 10000000 < 1000, ' edge', ''),
    if(number >= 10400000, ' tail', ''),
    if(number < 1000, ' needle', ''),
    if(number >= 10200000 AND number < 10210000, ' pin', ''))
FROM numbers(10500000);

OPTIMIZE TABLE tab FINAL;
SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT 10500000 + number, concat('every', if(number % 2 = 0, ' half', ''), if(number % 5 = 0, ' third', ''))
FROM numbers(200000);

SELECT count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active;

-- The large part has two slices and the small one has one, so all three slices are counted in parallel.
SELECT trimLeft(explain) FROM (EXPLAIN PIPELINE SELECT count() FROM tab WHERE hasAllTokens(text, ['half', 'third']) SETTINGS max_threads = 8)
WHERE explain LIKE '%TextIndexCount%';

SELECT 'max_threads = 1';
SELECT 'all', count() FROM tab WHERE hasAllTokens(text, ['half', 'third']) SETTINGS max_threads = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'all edge', count() FROM tab WHERE hasAllTokens(text, ['seventh', 'edge']) SETTINGS max_threads = 1, query_plan_optimize_count_from_text_index = 1;
SELECT 'any', count() FROM tab WHERE hasAnyTokens(text, ['seventh', 'tail']) SETTINGS max_threads = 1, query_plan_optimize_count_from_text_index = 1;

-- Two streams take turns on the slices.
SELECT 'max_threads = 2';
SELECT 'all', count() FROM tab WHERE hasAllTokens(text, ['half', 'third']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1;
SELECT 'all leapfrog', count() FROM tab WHERE hasAllTokens(text, ['half', 'third']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1, text_index_postings_intersection_algorithm = 'leapfrog';
SELECT 'all bruteforce', count() FROM tab WHERE hasAllTokens(text, ['half', 'third']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1, text_index_postings_intersection_algorithm = 'bruteforce';
SELECT 'all edge', count() FROM tab WHERE hasAllTokens(text, ['seventh', 'edge']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1;
SELECT 'all edge leapfrog', count() FROM tab WHERE hasAllTokens(text, ['seventh', 'edge']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1, text_index_postings_intersection_algorithm = 'leapfrog';
SELECT 'all tail', count() FROM tab WHERE hasAllTokens(text, ['third', 'tail']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1;
SELECT 'any', count() FROM tab WHERE hasAnyTokens(text, ['seventh', 'tail']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1;
SELECT 'any edge', count() FROM tab WHERE hasAnyTokens(text, ['edge', 'tail']) SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1;

SELECT 'max_threads = 8';
SELECT 'all', count() FROM tab WHERE hasAllTokens(text, ['half', 'third']) SETTINGS max_threads = 8, query_plan_optimize_count_from_text_index = 1, log_comment = '05401_all_sliced';
SELECT 'all edge', count() FROM tab WHERE hasAllTokens(text, ['seventh', 'edge']) SETTINGS max_threads = 8, query_plan_optimize_count_from_text_index = 1;
SELECT 'any', count() FROM tab WHERE hasAnyTokens(text, ['seventh', 'tail']) SETTINGS max_threads = 8, query_plan_optimize_count_from_text_index = 1;
SELECT 'single token', count() FROM tab WHERE hasToken(text, 'third') SETTINGS max_threads = 8, query_plan_optimize_count_from_text_index = 1;

SELECT 'normal read';
SELECT 'all', count() FROM tab WHERE hasAllTokens(text, ['half', 'third']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT 'all edge', count() FROM tab WHERE hasAllTokens(text, ['seventh', 'edge']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT 'all tail', count() FROM tab WHERE hasAllTokens(text, ['third', 'tail']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT 'any', count() FROM tab WHERE hasAnyTokens(text, ['seventh', 'tail']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT 'any edge', count() FROM tab WHERE hasAnyTokens(text, ['edge', 'tail']) SETTINGS query_plan_optimize_count_from_text_index = 0;
SELECT 'single token', count() FROM tab WHERE hasToken(text, 'third') SETTINGS query_plan_optimize_count_from_text_index = 0;

-- The index prunes granules by the rows of `needle` and `pin` at plan time, so the small part is skipped and the slices
-- of the large part are selected partially: they report the rows of the selected granules they cover, the same as
-- the normal read.
SET use_skip_indexes_on_data_read = 0;

SELECT 'selected ranges';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['needle', 'pin'])
SETTINGS max_threads = 2, query_plan_optimize_count_from_text_index = 1, log_comment = '05401_selected_ranges';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['needle', 'pin'])
SETTINGS query_plan_optimize_count_from_text_index = 0, log_comment = '05401_selected_ranges_off';

-- The slices report the rows of the selected ranges they cover, which add up to the rows of the parts.
SYSTEM FLUSH LOGS query_log;
SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05401_all_sliced';

-- The randomized MergeTree settings change the granules selected by `needle` and `pin`, so the read rows are compared
-- with the normal read.
SELECT countDistinct(read_rows), count() FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05401_selected_ranges', '05401_selected_ranges_off');

DROP TABLE tab;
