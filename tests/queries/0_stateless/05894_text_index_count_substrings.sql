-- Tests that a text index is used for `countSubstrings` and its case-insensitive variants like for `position`, and that
-- `positionCaseInsensitive` and `countSubstringsCaseInsensitive`, which fold the case of ASCII letters only, use the index
-- with the `array` tokenizer for a non-ASCII needle.
-- Every query is compared with use_skip_indexes = 0. Each row is a granule of its own, so `max_rows_to_read` fails
-- a query that does not skip granules.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 4;
SET use_query_condition_cache = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES
    (1, 'java.lang.NullPointerException at Main'), (2, 'java.lang.OutOfMemoryError: Java heap space'),
    (3, 'connection Timeout'), (4, 'all good'), (5, 'x.lang.y.lang.z'), (6, ''), (7, 'nothing here');

SELECT '-- comparisons equivalent to the needle being found';
SELECT '> 0', groupArray(id) FROM tab WHERE countSubstrings(msg, 'PointerExc') > 0 SETTINGS max_rows_to_read = 1;
SELECT '>= 1', groupArray(id) FROM tab WHERE countSubstrings(msg, 'PointerExc') >= 1 SETTINGS max_rows_to_read = 1;
SELECT '0 !=', groupArray(id) FROM tab WHERE 0 != countSubstrings(msg, 'PointerExc') SETTINGS max_rows_to_read = 1;
SELECT 'no index', groupArray(id) FROM tab WHERE countSubstrings(msg, 'PointerExc') > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- conditions that only imply the needle being found';
SELECT 'bare', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') SETTINGS max_rows_to_read = 3;
SELECT 'bare, no index', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') SETTINGS use_skip_indexes = 0;
SELECT '= 1', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') = 1 SETTINGS max_rows_to_read = 3;
SELECT '= 1, no index', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') = 1 SETTINGS use_skip_indexes = 0;
SELECT '>= 2', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') >= 2 SETTINGS max_rows_to_read = 3;
SELECT '>= 2, no index', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') >= 2 SETTINGS use_skip_indexes = 0;

SELECT '-- conditions that accept 0 cannot use the index';
SELECT '= 0', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') = 0;
SELECT '= 0, no index', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.') = 0 SETTINGS use_skip_indexes = 0;
SELECT 'start position', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.', 4) > 0;
SELECT 'start position, no index', groupArray(id) FROM tab WHERE countSubstrings(msg, '.lang.', 4) > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- the result of countSubstrings is kept where it is not a condition';
SELECT id, countSubstrings(msg, '.lang.'), countSubstrings(msg, '.lang.') > 0 FROM tab WHERE countSubstrings(msg, '.lang.') ORDER BY id;
SELECT id, countSubstrings(msg, 'PointerExc') FROM tab WHERE countSubstrings(msg, 'PointerExc') > 0 ORDER BY id;

SELECT '-- direct read replaces the equivalent comparison only';
SELECT '> 0', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION countSubstrings(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE countSubstrings(msg, 'PointerExc') > 0);
SELECT '= 1', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION countSubstrings(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE countSubstrings(msg, '.lang.') = 1);
SELECT 'bare', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION countSubstrings(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE countSubstrings(msg, '.lang.'));

SELECT '-- case-insensitive variants';
SELECT 'countSubstringsCaseInsensitive', groupArray(id) FROM tab WHERE countSubstringsCaseInsensitive(msg, 'OUTOFMEMORY') > 0 SETTINGS max_rows_to_read = 1;
SELECT 'countSubstringsCaseInsensitiveUTF8', groupArray(id) FROM tab WHERE countSubstringsCaseInsensitiveUTF8(msg, 'OUTOFMEMORY') > 0 SETTINGS max_rows_to_read = 1;
SELECT 'countSubstringsCaseInsensitive, no index', groupArray(id) FROM tab WHERE countSubstringsCaseInsensitive(msg, 'OUTOFMEMORY') > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- the variants without UTF8 take a non-ASCII needle with the array tokenizer';
CREATE TABLE tab_array
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab_array VALUES (1, 'CAFÉ'), (2, 'café'), (3, 'CAFé'), (4, 'zz');

-- 'É' and 'é' differ outside ASCII, so row 1 does not match.
SELECT 'countSubstringsCaseInsensitive', groupArray(id) FROM tab_array WHERE countSubstringsCaseInsensitive(msg, 'café') > 0 SETTINGS max_rows_to_read = 2;
SELECT 'countSubstringsCaseInsensitive, no index', groupArray(id) FROM tab_array WHERE countSubstringsCaseInsensitive(msg, 'café') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'instr', groupArray(id) FROM tab_array WHERE instr(msg, 'café') > 0 SETTINGS max_rows_to_read = 2;
-- The UTF8 variants also fold 'É', which the dictionary scan does not do, so they do not use the index.
SELECT 'countSubstringsCaseInsensitiveUTF8', groupArray(id) FROM tab_array WHERE countSubstringsCaseInsensitiveUTF8(msg, 'café') > 0;
SELECT 'countSubstringsCaseInsensitiveUTF8, no index', groupArray(id) FROM tab_array WHERE countSubstringsCaseInsensitiveUTF8(msg, 'café') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'countSubstringsCaseInsensitive', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION countSubstringsCaseInsensitive(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_array WHERE countSubstringsCaseInsensitive(msg, 'café') > 0);
SELECT 'countSubstringsCaseInsensitiveUTF8', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION countSubstringsCaseInsensitiveUTF8(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_array WHERE countSubstringsCaseInsensitiveUTF8(msg, 'café') > 0);

SELECT '-- a map element: the index only skips granules, and with query_plan_text_index_add_hint adds a hint';
CREATE TABLE tab_map
(
    id UInt32,
    m Map(String, String),
    INDEX idx(mapValues(m)) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
-- Row 1 has no key 'k', so m['k'] is ''. It holds 'hello' under another key and must not match.
INSERT INTO tab_map VALUES (1, map('other', 'hello')), (2, map('k', 'hello')), (3, map('k', 'world'));

SELECT 'map', groupArray(id) FROM tab_map WHERE countSubstrings(m['k'], 'hell') > 0 SETTINGS max_rows_to_read = 2;
SELECT 'map, no index', groupArray(id) FROM tab_map WHERE countSubstrings(m['k'], 'hell') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'map', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION countSubstrings(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_map WHERE countSubstrings(m['k'], 'hell') > 0 SETTINGS query_plan_text_index_add_hint = 1);

DROP TABLE tab_array;
DROP TABLE tab_map;
DROP TABLE tab;
