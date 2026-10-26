-- Tags: no-parallel-replicas
-- A LIKE scan that matches a token with embedded postings must not count them twice for another query on the same index.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET enable_full_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128)
)
ENGINE = MergeTree
ORDER BY id;

-- 'alpha' is in one row, so its postings are embedded; 'beta' spans several posting blocks.
INSERT INTO tab SELECT number, if(number = 0, 'alpha', 'beta alphabeta') FROM numbers(1000);

SELECT count() FROM tab WHERE hasAnyTokens(msg, ['alpha', 'beta']) AND msg LIKE '%alpha%';
SELECT count() FROM tab WHERE hasAnyTokens(msg, ['alpha', 'beta']) AND msg LIKE '%alpha%' SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasAnyTokens(msg, ['alpha', 'beta']) AND msg LIKE '%alpha%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE NOT hasAnyTokens(msg, ['alpha', 'beta']) AND msg LIKE '%alpha%';
SELECT count() FROM tab WHERE NOT hasAnyTokens(msg, ['alpha', 'beta']) AND msg LIKE '%alpha%' SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;
