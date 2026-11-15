-- Random settings limits: optimize_empty_string_comparisons=(1, None)
-- The text index preprocessor applies to a function evaluated row by row when its haystack is the index expression,
-- also when the expression has a constant lambda or a comparison with an empty string.

SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_post;
DROP TABLE IF EXISTS tab_phrase;

CREATE TABLE tab
(
    s String,
    a String ALIAS arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' '),
    INDEX idx_alias a TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(a)),
    INDEX idx_reverse arrayStringConcat(arrayMap(x -> reverse(x), splitByChar(' ', s)), ' ')
        TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arrayStringConcat(arrayMap(x -> reverse(x), splitByChar(' ', s)), ' '))),
    INDEX idx_nested arrayStringConcat(arrayMap(x -> arrayStringConcat(arrayMap(y -> concat(y, 'x'), [x])), splitByChar(' ', s)), ' ')
        TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arrayStringConcat(arrayMap(x -> arrayStringConcat(arrayMap(y -> concat(y, 'x'), [x])), splitByChar(' ', s)), ' '))),
    INDEX idx_array arrayMap(x -> concat(x, 'x'), splitByChar(' ', s))
        TYPE text(tokenizer = 'array', preprocessor = lower(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)))),
    INDEX idx_filter arrayStringConcat(arrayFilter(x -> x != '', splitByChar(' ', s)), ' ')
        TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arrayStringConcat(arrayFilter(x -> x != '', splitByChar(' ', s)), ' '))),
    INDEX idx_if if(s != '', s, 'none') TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(if(s != '', s, 'none'))),
    INDEX idx_column arrayStringConcat(arrayMap(x -> concat(x, s), splitByChar(' ', s)), ' ')
        TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arrayStringConcat(arrayMap(x -> concat(x, s), splitByChar(' ', s)), ' ')))
)
ENGINE = MergeTree ORDER BY s;

INSERT INTO tab VALUES ('Hello World'), ('FOO bar'), ('');

SELECT 'constant lambda in an ALIAS column';
SELECT s, hasToken(a, 'HELLOX') FROM tab ORDER BY s;
SELECT count() FROM tab WHERE hasToken(a, 'HELLOX');
SELECT count() FROM tab WHERE hasToken(a, 'HELLOX') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(a, 'HELLOX') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE NOT hasToken(a, 'HELLOX') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'lambda without captures';
SELECT s, hasToken(arrayStringConcat(arrayMap(x -> reverse(x), splitByChar(' ', s)), ' '), 'OLLEH') FROM tab ORDER BY s;
SELECT count() FROM tab WHERE hasToken(arrayStringConcat(arrayMap(x -> reverse(x), splitByChar(' ', s)), ' '), 'OLLEH') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'nested lambdas';
SELECT s, hasToken(arrayStringConcat(arrayMap(x -> arrayStringConcat(arrayMap(y -> concat(y, 'x'), [x])), splitByChar(' ', s)), ' '), 'HELLOX') FROM tab ORDER BY s;
SELECT count() FROM tab WHERE hasToken(arrayStringConcat(arrayMap(x -> arrayStringConcat(arrayMap(y -> concat(y, 'x'), [x])), splitByChar(' ', s)), ' '), 'HELLOX') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'array index';
SELECT s, hasAnyTokens(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ['hellox']), hasAllTokens(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ['hellox', 'worldx']) FROM tab ORDER BY s;
SELECT count() FROM tab WHERE hasAnyTokens(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ['hellox']) SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'comparison with an empty string in a lambda';
SELECT s, hasToken(arrayStringConcat(arrayFilter(x -> x != '', splitByChar(' ', s)), ' '), 'HELLO') FROM tab ORDER BY s;
SELECT count() FROM tab WHERE hasToken(arrayStringConcat(arrayFilter(x -> x != '', splitByChar(' ', s)), ' '), 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'comparison with an empty string';
SELECT s, hasToken(if(s != '', s, 'none'), 'HELLO') FROM tab ORDER BY s;
SELECT count() FROM tab WHERE hasToken(if(s != '', s, 'none'), 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(if(s != '', s, 'none'), 'HELLO') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasToken(if(s != '', s, 'none'), 'HELLO') SETTINGS query_plan_direct_read_from_text_index = 0, optimize_empty_string_comparisons = 0;

SELECT 'lambda capturing a column';
SELECT s, hasToken(arrayStringConcat(arrayMap(x -> concat(x, s), splitByChar(' ', s)), ' '), 'HELLOHELLO') FROM tab ORDER BY s;
SELECT count() FROM tab WHERE hasToken(arrayStringConcat(arrayMap(x -> concat(x, s), splitByChar(' ', s)), ' '), 'HELLOHELLO') SETTINGS query_plan_direct_read_from_text_index = 0;

CREATE TABLE tab_post
(
    s String,
    INDEX idx arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' ')
        TYPE text(tokenizer = 'splitByNonAlpha',
                  preprocessor = lower(arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' ')),
                  postprocessor = reverse(arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' ')))
)
ENGINE = MergeTree ORDER BY s;

INSERT INTO tab_post VALUES ('Hello World'), ('FOO bar'), ('');

SELECT 'preprocessor and postprocessor';
SELECT s, hasToken(arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' '), 'HELLOX') FROM tab_post ORDER BY s;
SELECT count() FROM tab_post WHERE hasToken(arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' '), 'HELLOX') SETTINGS query_plan_direct_read_from_text_index = 0;

CREATE TABLE tab_phrase
(
    s String,
    INDEX idx arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' ')
        TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' ')), support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY s
SETTINGS allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_phrase VALUES ('Hello World'), ('FOO bar'), ('');

SELECT 'hasPhrase';
SELECT s, hasPhrase(arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' '), 'HELLOX WORLDX') FROM tab_phrase ORDER BY s;
SELECT count() FROM tab_phrase WHERE hasPhrase(arrayStringConcat(arrayMap(x -> concat(x, 'x'), splitByChar(' ', s)), ' '), 'HELLOX WORLDX') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;
DROP TABLE tab_post;
DROP TABLE tab_phrase;
