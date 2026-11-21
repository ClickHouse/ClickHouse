-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Without `support_phrase_search`, `hasPhrase` with a needle of exactly one token is the same as `hasAllTokens`,
-- so direct read from the text index is exact and the row-level `hasPhrase` is not evaluated.
-- Needles of several tokens, including a repeated token such as 'a a', still need the row-level adjacency check.

SET enable_full_text_index = 1;
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    w String,
    INDEX idx w TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (1, 'c++ developer'), (2, 'senior c# engineer'), (3, 'a a b'), (4, 'a b a'), (5, 'patata');

SELECT '-- results match with and without the index';

SELECT 'c++', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'c++');
SELECT 'c++', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'c++') SETTINGS use_skip_indexes = 0;
SELECT 'a', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'a');
SELECT 'a', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'a') SETTINGS use_skip_indexes = 0;
SELECT 'a a', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'a a');
SELECT 'a a', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'a a') SETTINGS use_skip_indexes = 0;
SELECT 'senior c#', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'senior c#');
SELECT 'senior c#', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'senior c#') SETTINGS use_skip_indexes = 0;
SELECT '[c++]', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, ['c++']);
SELECT '[c++]', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, ['c++']) SETTINGS use_skip_indexes = 0;
SELECT '[a, a]', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, ['a', 'a']);
SELECT '[a, a]', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, ['a', 'a']) SETTINGS use_skip_indexes = 0;
SELECT 'c++ or c#', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'c++') OR hasPhrase(w, 'c#');
SELECT 'c++ or c#', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'c++') OR hasPhrase(w, 'c#') SETTINGS use_skip_indexes = 0;

SELECT '-- one token: only the text index virtual column; several tokens: the row-level hasPhrase stays';

SELECT 'c++', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, 'c++'));
SELECT 'a', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, 'a'));
SELECT 'a a', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, 'a a'));
SELECT 'senior c#', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, 'senior c#'));
SELECT '[c++]', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, ['c++']));
SELECT '[a, a]', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, ['a', 'a']));
SELECT 'c++ or c#', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, 'c++') OR hasPhrase(w, 'c#'));

DROP TABLE tab;

SELECT '-- the token count is taken after the preprocessor';

CREATE TABLE tab
(
    id UInt64,
    w String,
    INDEX idx w TYPE text(tokenizer = splitByString([' ']), preprocessor = lower(w))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES (1, 'C++ Lead'), (2, 'c++ dev'), (3, 'Java');

SELECT 'C++', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'C++');
SELECT 'C++', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'C++') SETTINGS use_skip_indexes = 0;
SELECT 'C++ LEAD', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'C++ LEAD');
SELECT 'C++ LEAD', arraySort(groupArray(id)) FROM tab WHERE hasPhrase(w, 'C++ LEAD') SETTINGS use_skip_indexes = 0;

SELECT 'C++', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, 'C++'));
SELECT 'C++ LEAD', countIf(explain LIKE '%__text_index_idx_hasPhrase%') > 0, countIf(explain LIKE '%FUNCTION hasPhrase(%') > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasPhrase(w, 'C++ LEAD'));

DROP TABLE tab;
