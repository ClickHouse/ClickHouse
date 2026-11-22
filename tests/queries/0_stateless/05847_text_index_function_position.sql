-- Tests that a text index is used for `position`, `positionUTF8`, `positionCaseInsensitive` and `positionCaseInsensitiveUTF8`,
-- used directly as a condition or compared with a constant so that the comparison rejects 0 (the needle is not found).
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
DROP TABLE IF EXISTS tab_null;

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
    (3, 'connection Timeout'), (4, 'all good'), (5, 'nullpointerexception in lower case'), (6, '');

SELECT '-- comparisons equivalent to the needle being found';
SELECT '> 0', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') > 0 SETTINGS max_rows_to_read = 1;
SELECT '!= 0', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') != 0 SETTINGS max_rows_to_read = 1;
SELECT '>= 1', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') >= 1 SETTINGS max_rows_to_read = 1;
SELECT '> 0.5', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') > 0.5 SETTINGS max_rows_to_read = 1;
SELECT '> -0', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') > toInt8(0) SETTINGS max_rows_to_read = 1;
SELECT '0 <', groupArray(id) FROM tab WHERE 0 < position(msg, 'PointerExc') SETTINGS max_rows_to_read = 1;
SELECT '1 <=', groupArray(id) FROM tab WHERE 1 <= position(msg, 'PointerExc') SETTINGS max_rows_to_read = 1;
SELECT '0 !=', groupArray(id) FROM tab WHERE 0 != position(msg, 'PointerExc') SETTINGS max_rows_to_read = 1;
SELECT 'no index', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- conditions that only imply the needle being found';
-- They do not use the dictionary scan, which pays off only when the index replaces the predicate: the tokens inside
-- the needle are searched instead.
SELECT 'bare', groupArray(id) FROM tab WHERE position(msg, '.lang.') SETTINGS max_rows_to_read = 2;
SELECT 'bare, no index', groupArray(id) FROM tab WHERE position(msg, '.lang.') SETTINGS use_skip_indexes = 0;
SELECT '= 5', groupArray(id) FROM tab WHERE position(msg, '.lang.') = 5 SETTINGS max_rows_to_read = 2;
SELECT '= 5, no index', groupArray(id) FROM tab WHERE position(msg, '.lang.') = 5 SETTINGS use_skip_indexes = 0;
SELECT '= 3', groupArray(id) FROM tab WHERE position(msg, '.lang.') = 3 SETTINGS max_rows_to_read = 2;
SELECT '= 3, no index', groupArray(id) FROM tab WHERE position(msg, '.lang.') = 3 SETTINGS use_skip_indexes = 0;
SELECT '> 4', groupArray(id) FROM tab WHERE position(msg, '.lang.') > 4 SETTINGS max_rows_to_read = 2;
SELECT '> 4, no index', groupArray(id) FROM tab WHERE position(msg, '.lang.') > 4 SETTINGS use_skip_indexes = 0;
SELECT 'no tokens', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') = 15;
SELECT 'no tokens, no index', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') = 15 SETTINGS use_skip_indexes = 0;
SELECT 'between', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') < 20 AND position(msg, 'PointerExc') > 0 SETTINGS max_rows_to_read = 1;

SELECT '-- conditions that accept 0 cannot use the index';
SELECT '= 0', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') = 0;
SELECT '= 0, no index', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') = 0 SETTINGS use_skip_indexes = 0;
SELECT '< 5', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') < 5;
SELECT '< 5, no index', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') < 5 SETTINGS use_skip_indexes = 0;
SELECT '>= 0', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') >= 0;
SELECT '>= 0, no index', groupArray(id) FROM tab WHERE position(msg, 'PointerExc') >= 0 SETTINGS use_skip_indexes = 0;
SELECT 'not', groupArray(id) FROM tab WHERE NOT position(msg, 'PointerExc') > 0;
SELECT 'not, no index', groupArray(id) FROM tab WHERE NOT position(msg, 'PointerExc') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'empty needle', groupArray(id) FROM tab WHERE position(msg, '') > 0;
SELECT 'empty needle, no index', groupArray(id) FROM tab WHERE position(msg, '') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'start position', groupArray(id) FROM tab WHERE position(msg, 'PointerExc', 20) > 0;
SELECT 'start position, no index', groupArray(id) FROM tab WHERE position(msg, 'PointerExc', 20) > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- variants';
SELECT 'positionUTF8', groupArray(id) FROM tab WHERE positionUTF8(msg, 'PointerExc') > 0 SETTINGS max_rows_to_read = 1;
SELECT 'positionCaseInsensitive', groupArray(id) FROM tab WHERE positionCaseInsensitive(msg, 'OUTOFMEMORY') > 0 SETTINGS max_rows_to_read = 1;
SELECT 'positionCaseInsensitive, no index', groupArray(id) FROM tab WHERE positionCaseInsensitive(msg, 'OUTOFMEMORY') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'positionCaseInsensitiveUTF8', groupArray(id) FROM tab WHERE positionCaseInsensitiveUTF8(msg, 'pointerEXC') != 0 SETTINGS max_rows_to_read = 2;
SELECT 'positionCaseInsensitiveUTF8, no index', groupArray(id) FROM tab WHERE positionCaseInsensitiveUTF8(msg, 'pointerEXC') != 0 SETTINGS use_skip_indexes = 0;

SELECT '-- the result of position is kept where it is not a condition';
SELECT id, position(msg, 'PointerExc'), position(msg, 'PointerExc') > 0 FROM tab WHERE position(msg, 'PointerExc') ORDER BY id;
SELECT id, position(msg, 'PointerExc') FROM tab WHERE position(msg, 'PointerExc') > 0 ORDER BY id;
SELECT id, position(msg, '.lang.') + 1 > 1 FROM tab WHERE position(msg, '.lang.') = 5 ORDER BY id;

SELECT '-- without the dictionary scan, the tokens inside the needle are searched';
SELECT 'tokens', groupArray(id) FROM tab WHERE position(msg, '.lang.') > 0 SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, max_rows_to_read = 2;
SELECT 'tokens, no index', groupArray(id) FROM tab WHERE position(msg, '.lang.') > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- direct read replaces the equivalent comparison only';
SELECT '> 0', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(msg, 'PointerExc') > 0);
SELECT '= 5', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(msg, '.lang.') = 5);
SELECT 'bare', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION position(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE position(msg, '.lang.'));

SELECT '-- Nullable';
CREATE TABLE tab_null
(
    id UInt32,
    msg Nullable(String),
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab_null SELECT id, if(id % 2 = 0, NULL, msg) FROM tab;

SELECT 'Nullable', groupArray(id) FROM tab_null WHERE position(msg, '.lang.') > 0;
SELECT 'Nullable, no index', groupArray(id) FROM tab_null WHERE position(msg, '.lang.') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'Nullable, not', groupArray(id) FROM tab_null WHERE NOT position(msg, '.lang.') > 0;
SELECT 'Nullable, not, no index', groupArray(id) FROM tab_null WHERE NOT position(msg, '.lang.') > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- a preprocessor that does not preserve substrings';
DROP TABLE IF EXISTS tab_preprocessor;
CREATE TABLE tab_preprocessor
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, preprocessor = substring(s, 1, 10)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();
INSERT INTO tab_preprocessor VALUES ('0123456789 .needle.');
SELECT '> 0', count() FROM tab_preprocessor WHERE position(s, '.needle.') > 0;
SELECT '> 0, no index', count() FROM tab_preprocessor WHERE position(s, '.needle.') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'bare', count() FROM tab_preprocessor WHERE position(s, '.needle.');
SELECT 'positionUTF8', count() FROM tab_preprocessor WHERE positionUTF8(s, '.needle.') > 0;
SELECT 'positionCaseInsensitive', count() FROM tab_preprocessor WHERE positionCaseInsensitive(s, '.NEEDLE.') > 0;

SELECT '-- a case-changing preprocessor';
DROP TABLE IF EXISTS tab_lower;
CREATE TABLE tab_lower
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab_lower SELECT id, msg FROM tab;
SELECT 'tokens', groupArray(id) FROM tab_lower WHERE position(msg, '.Lang.') OR position(msg, '.lang.') SETTINGS max_rows_to_read = 2;
SELECT 'tokens, no index', groupArray(id) FROM tab_lower WHERE position(msg, '.Lang.') OR position(msg, '.lang.') SETTINGS use_skip_indexes = 0;

SELECT '-- asciiCJK: a connector on the edge of the needle joins tokens depending on the character outside of it';
DROP TABLE IF EXISTS tab_ascii_cjk;
CREATE TABLE tab_ascii_cjk
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = asciiCJK) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab_ascii_cjk VALUES
    (1, 'xx.lang.yy'), (2, 'a lang b'), (3, 'it\'s.lang:yy'), (4, '1.5.7'), (5, 'see -.lang.- here now'), (6, 'nothing'),
    (7, concat(unhex('F0'), ' lang end now'));
SELECT '.lang.', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '.lang.') > 0;
SELECT '.lang., no index', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '.lang.') > 0 SETTINGS use_skip_indexes = 0;
SELECT '.lang., bare', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '.lang.');
SELECT '.lang., UTF8', groupArray(id) FROM tab_ascii_cjk WHERE positionUTF8(msg, '.lang.') > 0;
SELECT '.lang., multiSearchAny', groupArray(id) FROM tab_ascii_cjk WHERE multiSearchAny(msg, ['.lang.']);
SELECT '.lang:', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '.lang:') > 0;
SELECT '.lang:, no index', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '.lang:') > 0 SETTINGS use_skip_indexes = 0;
SELECT 's.lang', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '\'s.lang') > 0;
SELECT 's.lang, no index', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '\'s.lang') > 0 SETTINGS use_skip_indexes = 0;
SELECT '.5.', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '.5.') > 0;
SELECT '.5., no index', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, '.5.') > 0 SETTINGS use_skip_indexes = 0;
SELECT '-- the connector is inside the needle: the token is still used';
SELECT '-.lang.-', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, 'see -.lang.- here ') > 0 SETTINGS max_rows_to_read = 1;
SELECT '-.lang.-, no index', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, 'see -.lang.- here ') > 0 SETTINGS use_skip_indexes = 0;
SELECT '-- a malformed leading byte before the needle';
SELECT 'malformed', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, ' lang end ') > 0;
SELECT 'malformed, no index', groupArray(id) FROM tab_ascii_cjk WHERE position(msg, ' lang end ') > 0 SETTINGS use_skip_indexes = 0;

SELECT '-- values that are not valid UTF-8: a leading byte before the needle swallows its first bytes';
DROP TABLE IF EXISTS tab_malformed;
CREATE TABLE tab_malformed
(
    id UInt32,
    msg String,
    msg_sparse String DEFAULT msg,
    INDEX idx_ngrams(msg) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX idx_sparse_grams(msg_sparse) TYPE text(tokenizer = sparseGrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
INSERT INTO tab_malformed (id, msg) VALUES
    (1, concat(unhex('C2'), 'needle')), (2, concat(unhex('E0'), 'needle')), (3, concat(unhex('F0'), 'needle')),
    (4, concat('a needle', unhex('F0'))), (5, concat(unhex('F0'), 'привет, needle')), (6, concat(unhex('F0'), 'nee')),
    (7, 'unrelated'), (8, 'another one');
SELECT 'ngrams', groupArray(id) FROM tab_malformed WHERE position(msg, 'needle') > 0 SETTINGS max_rows_to_read = 6;
SELECT 'sparseGrams', groupArray(id) FROM tab_malformed WHERE position(msg_sparse, 'needle') > 0;
SELECT 'no index', groupArray(id) FROM tab_malformed WHERE position(msg, 'needle') > 0 SETTINGS use_skip_indexes = 0;
SELECT 'ngrams, bare', groupArray(id) FROM tab_malformed WHERE position(msg, 'needle');
SELECT 'sparseGrams, bare', groupArray(id) FROM tab_malformed WHERE position(msg_sparse, 'needle');
SELECT 'ngrams, UTF8', groupArray(id) FROM tab_malformed WHERE positionUTF8(msg, 'needle') > 0;
SELECT 'sparseGrams, UTF8', groupArray(id) FROM tab_malformed WHERE positionUTF8(msg_sparse, 'needle') > 0;
SELECT 'ngrams, non-ASCII', groupArray(id) FROM tab_malformed WHERE position(msg, 'привет, needle') > 0 SETTINGS max_rows_to_read = 1;
SELECT 'sparseGrams, non-ASCII', groupArray(id) FROM tab_malformed WHERE position(msg_sparse, 'привет, needle') > 0;
SELECT 'non-ASCII, no index', groupArray(id) FROM tab_malformed WHERE position(msg, 'привет, needle') > 0 SETTINGS use_skip_indexes = 0;
SELECT '-- a needle of which nothing is left to search for';
SELECT 'ngrams, short', groupArray(id) FROM tab_malformed WHERE position(msg, 'nee') > 0;
SELECT 'short, no index', groupArray(id) FROM tab_malformed WHERE position(msg, 'nee') > 0 SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
DROP TABLE tab_null;
DROP TABLE tab_preprocessor;
DROP TABLE tab_lower;
DROP TABLE tab_ascii_cjk;
DROP TABLE tab_malformed;
