-- Correctness of a projection text index in several corner cases.

SET allow_experimental_projection_text_index = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_map;

SELECT 'Two predicates sharing a token';
-- Filling the virtual column of one predicate must not advance the posting list cursor of the
-- shared token `b` past the rows that the other predicate has yet to see.
CREATE TABLE tab (id UInt64, s String, PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id;
INSERT INTO tab SELECT number, multiIf(number IN (10, 30), 'a b', number IN (20, 40), 'c b', number IN (5, 50), 'b', 'x') FROM numbers(60);

SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(s, ['a', 'b']) OR hasAllTokens(s, ['c', 'b']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab WHERE hasAllTokens(s, ['a', 'b']) OR hasAllTokens(s, ['c', 'b']) ORDER BY id)
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT 'multiSearchAny evaluated by a dictionary scan';
-- The projection reader cannot evaluate a pattern-only query, so the original predicate must be kept.
SELECT count() FROM tab WHERE multiSearchAny(s, ['b']) SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT count() FROM tab WHERE multiSearchAny(s, ['b']) SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT 'A physical column shadowing a Map subcolumn';
CREATE TABLE tab_map (m Map(String, String), `m.key_nokey` String, PROJECTION idx INDEX mapKeys(m) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab_map VALUES ({'abc' : 'x'}, 'hello');

SELECT count() FROM tab_map WHERE `m.key_nokey` = 'hello';
SELECT count() FROM tab_map WHERE mapContainsKey(m, 'abc');
SELECT count() FROM tab_map WHERE mapContainsKey(m, 'nokey');

DROP TABLE tab_map;

SELECT 'Unsupported arguments';
CREATE TABLE tab (s String, PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError NOT_IMPLEMENTED }
CREATE TABLE tab (s String, PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha', enable_phrase_query_support = 1, has_block_index = 0))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (m Map(String, String), PROJECTION idx INDEX m TYPE text(tokenizer = 'keyValuePairs'))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError NOT_IMPLEMENTED }
CREATE TABLE tab (s String, PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 536870912))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (s String, PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 4294967296))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
