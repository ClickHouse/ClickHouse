-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 2;

SELECT 'Negative tests';

SELECT hasTokenStartingWith(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasTokenStartingWith('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasTokenStartingWith('a', 'b', 'splitByNonAlpha', 'c'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasTokenStartingWith(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasTokenStartingWith('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasTokenStartingWith('a', ['a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasTokenStartingWith('a', materialize('a')); -- { serverError ILLEGAL_COLUMN }
SELECT hasTokenStartingWith('a', 'a', 'no_such_tokenizer'); -- { serverError BAD_ARGUMENTS }

SELECT 'Without an index, tokens are split by non-alphanumeric characters';

SELECT hasTokenStartingWith('foo bar', 'ba');
SELECT hasTokenStartingWith('foo bar', 'bar');
SELECT hasTokenStartingWith('foo bar', 'ar');
SELECT hasTokenStartingWith('foo bar', 'barn');
SELECT hasTokenStartingWith('foo bar', '');
SELECT hasTokenStartingWith('', '');
SELECT hasTokenStartingWith(materialize('foo bar'), 'fo');
SELECT hasTokenStartingWith(toFixedString('foo bar', 7), 'ba');
SELECT hasTokenStartingWith(toNullable('foo bar'), 'ba');
SELECT hasTokenStartingWith(['foo', 'bar'], 'ba');
SELECT hasTokenStartingWith(['foo', 'bar'], 'ar');
SELECT hasTokenStartingWith([], 'a');
SELECT hasTokenStartingWith(['foo', NULL], 'fo');

SELECT 'A custom tokenizer via the 3rd argument';

SELECT hasTokenStartingWith('a|fab@x.com|c', 'fab@', 'splitByString([''|''])');
SELECT hasTokenStartingWith('a|fab@x.com|c', 'ab@', 'splitByString([''|''])');
SELECT hasTokenStartingWith('a|fab@x.com|c', 'fab@', 'splitByNonAlpha');
SELECT hasTokenStartingWith('abcdef', 'cde', 'ngrams(3)');
SELECT hasTokenStartingWith('abcdef', 'cdef', 'ngrams(3)');

SELECT 'Text index with the splitByNonAlpha tokenizer';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'alpha service prod'),
    (2, 'beta service prod'),
    (3, 'gamma svc dev'),
    (4, 'prod alpha'),
    (5, 'alphabet soup'),
    (6, 'scalpha');

SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alpha');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alphab');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'pro');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'lpha');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'none');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'a');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, '');
SELECT groupArray(id) FROM tab WHERE NOT hasTokenStartingWith(message, 'alph');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph') AND hasTokenStartingWith(message, 'pro');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'gam') OR hasTokenStartingWith(message, 'bet');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph', 'splitByNonAlpha');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph') SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph') SETTINGS text_index_like_min_pattern_length = 10;
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph') SETTINGS text_index_like_max_postings_to_read = 0;

SELECT 'The index decides the query on its own, so the original condition is removed';

-- Columns: does the plan have a text index virtual column, does it still evaluate the original function.
SELECT 'prefix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(message, 'alph'));

SELECT 'index tokenizer', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(message, 'alph', 'splitByNonAlpha'));

SELECT 'The function is left to the column scan, with the tokenizer of the index';

-- Third column: the tokenizer of the index was injected into the function.
SELECT 'too short', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0, countIf(explain LIKE '%splitByNonAlpha%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(message, 'a'));

SELECT 'scan disabled', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0, countIf(explain LIKE '%splitByNonAlpha%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(message, 'alph') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0);

SELECT 'other tokenizer', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(message, 'alph', 'ngrams(3)'));

SELECT 'Text index with a preprocessor: it is applied to the haystack and to the prefix';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'Alpha Service'), (2, 'beta'), (3, 'ALPHABET');

SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'ALPH');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'ALPH') SETTINGS use_skip_indexes = 0;

SELECT 'preprocessor', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(message, 'ALPH'));

SELECT 'Text index with a postprocessor: the index is not used, the function sees the tokens before postprocessing';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, postprocessor = upper(message))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'alpha service'), (2, 'beta');

SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'alph');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'ALPH');

SELECT 'postprocessor', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(message, 'alph'));

SELECT 'Array column with the array tokenizer: every element is a token';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    fields Array(String),
    INDEX idx(fields) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, ['fab@x.com', 'john']), (2, ['alf', 'abc']), (3, ['xfa', 'y']), (4, []);

SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(fields, 'fa');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(fields, 'fab@x.c');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(fields, 'ab');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(fields, 'jo');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(fields, 'a');

SELECT 'array', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(fields, 'fa'));

SELECT 'Fields joined with a separator and the splitByString tokenizer: prefix search over all fields';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    user_name String,
    email String,
    search String MATERIALIZED lower(concatWithSeparator('|', '', user_name, email)),
    INDEX idx(search) TYPE text(tokenizer = splitByString(['|']))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'john', 'Fab@x.com'), (2, 'alf', 'abc'), (3, 'xfa', 'y'), (4, 'FAy', '');

SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(search, 'fa');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(search, 'fab@');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(search, 'xf');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(search, 'ab');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(search, '|');
SELECT groupArray(id) FROM tab WHERE startsWith(search, 'fa');

SELECT 'splitByString', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(search, 'fa'));

SELECT 'A preprocessor that makes the value Nullable: the index is not used, NULL stays NULL under NOT';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx(s) TYPE text(tokenizer = array, preprocessor = nullIf(s, 'skip'))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'far'), (2, 'skip'), (3, 'bar');

SELECT groupArray(id) FROM tab WHERE NOT hasTokenStartingWith(s, 'far');
SELECT groupArray(id) FROM tab WHERE NOT hasTokenStartingWith(s, 'far') SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE NOT hasTokenStartingWith(toNullable(s), 'far');
SELECT groupArray(id) FROM tab WHERE NOT hasTokenStartingWith(toNullable(s), 'far') SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab PREWHERE NOT hasTokenStartingWith(s, 'far');
SELECT groupArray(id) FROM tab PREWHERE NOT hasTokenStartingWith(s, 'far') SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(s, 'far');
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(s, 'far') SETTINGS use_skip_indexes = 0;

SELECT 'nullable preprocessor', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION hasTokenStartingWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasTokenStartingWith(s, 'far'));

SELECT 'A two-character prefix seeks the dictionary to its key range instead of scanning every block';

SET log_queries = 1;
SET log_profile_events = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS tab;

-- dictionary_block_size = 4 with the 16 tokens below gives 4 dictionary blocks whose first tokens are
-- 'aaa0', 'bbb0', 'ccc0' and 'ddd0', so the key ranges of 'fa' and 'xy' both lie inside the last block.
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'aaa0'), (2, 'aaa1'), (3, 'aaa2'), (4, 'aaa3'),
    (5, 'bbb0'), (6, 'bbb1'), (7, 'bbb2'), (8, 'bbb3'),
    (9, 'ccc0'), (10, 'ccc1'), (11, 'ccc2'), (12, 'ccc3'),
    (13, 'ddd0'), (14, 'fa0'), (15, 'fa1'), (16, 'xyz0');

SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'fa') SETTINGS log_comment = '05443_prefix_fa';
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'xy') SETTINGS log_comment = '05443_prefix_xy';
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'fa') SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE hasTokenStartingWith(message, 'xy') SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND log_comment IN ('05443_prefix_fa', '05443_prefix_xy')
ORDER BY log_comment;

DROP TABLE tab;
