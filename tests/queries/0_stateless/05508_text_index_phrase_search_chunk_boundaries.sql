-- `hasPhrase` on the positions index over several chunks of candidates must match evaluating the column.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_codec = 'pfor')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1, index_granularity = 128;

INSERT INTO tab
SELECT number, arrayStringConcat(arrayMap(i -> ['the', 'quick', 'brown', 'fox', 'the', 'the', 'quick', 'dog'][1 + cityHash64(number, i) % 8], range(1 + number % 20)), ' ')
FROM numbers(200000);

OPTIMIZE TABLE tab FINAL;

SELECT 'Two terms';
SELECT count() FROM tab WHERE hasPhrase(s, 'the quick') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, text_index_hint_max_selectivity = 1;
SELECT count() FROM tab WHERE hasPhrase(s, 'the quick') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Repeated term';
SELECT count() FROM tab WHERE hasPhrase(s, 'the the') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, text_index_hint_max_selectivity = 1;
SELECT count() FROM tab WHERE hasPhrase(s, 'the the') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Three terms';
SELECT count() FROM tab WHERE hasPhrase(s, 'the quick brown') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, text_index_hint_max_selectivity = 1;
SELECT count() FROM tab WHERE hasPhrase(s, 'the quick brown') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Three repeated terms';
SELECT count() FROM tab WHERE hasPhrase(s, 'the the the') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, text_index_hint_max_selectivity = 1;
SELECT count() FROM tab WHERE hasPhrase(s, 'the the the') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'Four terms';
SELECT count() FROM tab WHERE hasPhrase(s, 'brown dog the quick') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, text_index_hint_max_selectivity = 1;
SELECT count() FROM tab WHERE hasPhrase(s, 'brown dog the quick') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;
