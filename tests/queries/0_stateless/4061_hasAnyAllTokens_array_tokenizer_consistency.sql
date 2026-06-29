-- Tags: no-parallel-replicas
-- Test for https://github.com/ClickHouse/ClickHouse/issues/108808
-- hasAnyTokens / hasAllTokens on an Array(String) column with a text(tokenizer = 'array') index
-- must return the same result regardless of whether the text-index filter path is used.
-- The row-level / projection path used to default to the splitByNonAlpha tokenizer (which splits
-- 'foo-bar' into 'foo','bar') while the index keeps the element whole, so countIf(hasAnyTokens(...))
-- and `SETTINGS use_skip_indexes = 0` silently returned 0 instead of 47. All paths must agree.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_4061;

CREATE TABLE t_4061
(
    id UInt64,
    tags Array(String),
    INDEX idx_tags_text tags TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_4061
SELECT number, if(number < 47, ['foo-bar'], ['baz'])
FROM numbers(100000);

SELECT 'has baseline projection';
SELECT countIf(has(tags, 'foo-bar')) FROM t_4061;

SELECT 'hasAnyTokens projection';
SELECT countIf(hasAnyTokens(tags, ['foo-bar'])) FROM t_4061;

SELECT 'hasAllTokens projection';
SELECT countIf(hasAllTokens(tags, ['foo-bar'])) FROM t_4061;

SELECT 'hasAnyTokens WHERE use_skip_indexes=1';
SELECT count() FROM t_4061 WHERE hasAnyTokens(tags, ['foo-bar']) SETTINGS use_skip_indexes = 1;

SELECT 'hasAnyTokens WHERE use_skip_indexes=0';
SELECT count() FROM t_4061 WHERE hasAnyTokens(tags, ['foo-bar']) SETTINGS use_skip_indexes = 0;

SELECT 'hasAllTokens WHERE use_skip_indexes=1';
SELECT count() FROM t_4061 WHERE hasAllTokens(tags, ['foo-bar']) SETTINGS use_skip_indexes = 1;

SELECT 'hasAllTokens WHERE use_skip_indexes=0';
SELECT count() FROM t_4061 WHERE hasAllTokens(tags, ['foo-bar']) SETTINGS use_skip_indexes = 0;

SELECT 'hasAnyTokens projection explicit array tokenizer (workaround)';
SELECT countIf(hasAnyTokens(tags, ['foo-bar'], 'array')) FROM t_4061;

DROP TABLE t_4061;
