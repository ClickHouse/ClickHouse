-- Tests `hasPhrase`, `hasAnyTokens`, and `hasAllTokens` on JSON paths covered by a `JSONAllValues` text index.
-- See issue https://github.com/ClickHouse/ClickHouse/issues/123427.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET query_plan_direct_read_from_text_index = 1;

CREATE TABLE tab_split_by_string
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_split_by_non_alpha
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_ngrams
(
  id UInt32,
  data JSON,
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = ngrams(3))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_typed
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_array
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_split_by_string
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_separator
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_array_path_postprocessor
(
  id UInt32,
  data JSON(key Array(String)),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_preprocessor
(
  id UInt32,
  data JSON(key String),
  INDEX idx JSONAllValues(data) TYPE text(tokenizer = splitByString([' ']), preprocessor = lower(JSONAllValues(data)))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_split_by_string VALUES (1, '{"key": "the quick brown fox"}'), (2, '{"key": "lazy dog"}'), (3, '{"key": "brown quick"}');
INSERT INTO tab_split_by_non_alpha SELECT * FROM tab_split_by_string;
INSERT INTO tab_array SELECT * FROM tab_split_by_string;
INSERT INTO tab_ngrams SELECT * FROM tab_split_by_string;
INSERT INTO tab_typed SELECT * FROM tab_split_by_string;
INSERT INTO tab_array_path_array VALUES (1, '{"key": ["the quick brown fox", "cat"]}'), (2, '{"key": ["lazy", "dog"]}');
INSERT INTO tab_array_path_split_by_string SELECT * FROM tab_array_path_array;
INSERT INTO tab_array_path_separator VALUES (1, '{"key": ["the quick()brown fox"]}'), (2, '{"key": ["lazy dog"]}');
INSERT INTO tab_preprocessor VALUES (1, '{"key": "QUICK BROWN fox"}'), (2, '{"key": "lazy dog"}');
INSERT INTO tab_array_path_postprocessor VALUES (1, '{"key": ["CAT"]}'), (2, '{"key": ["dog"]}');

SELECT 'splitByString';
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, 'quick brown') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, 'quick brown') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'splitByString, index condition';
SELECT trim(explain) FROM (
  EXPLAIN indexes = 1
  SELECT id FROM tab_split_by_string WHERE hasPhrase(data.key::String, ['quick', 'brown'])
)
WHERE explain LIKE '%tokens:%'
SETTINGS enable_parallel_replicas = 0;

SELECT 'splitByNonAlpha';
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_non_alpha WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_split_by_non_alpha WHERE hasPhrase(data.key::String, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array';
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['quick', 'brown'], 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['quick', 'brown'], 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['the quick brown fox'], 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array WHERE hasPhrase(data.key::String, ['the quick brown fox'], 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'ngrams';
SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasPhrase(data.key::String, ['qui', 'uic', 'ick'], 'ngrams(3)') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_ngrams WHERE hasPhrase(data.key::String, ['qui', 'uic', 'ick'], 'ngrams(3)') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'typed path';
SELECT groupArray(id) FROM (SELECT id FROM tab_typed WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_typed WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array-valued path, array';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, ['the quick brown fox'], 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, ['the quick brown fox'], 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, 'dog', 'array') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasPhrase(data.key, 'dog', 'array') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAllTokens(data.key, ['the quick brown fox', 'cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAllTokens(data.key, ['the quick brown fox', 'cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, 'dog') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_array WHERE hasAnyTokens(data.key, 'dog') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array-valued path, splitByString';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, ['quick', 'brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, 'quick brown') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasPhrase(data.key, 'quick brown') ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAnyTokens(data.key, ['dog', 'bird']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, ['quick', 'cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, ['quick', 'cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, 'quick cat') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_split_by_string WHERE hasAllTokens(data.key, 'quick cat') ORDER BY id) SETTINGS use_skip_indexes = 0;

SELECT 'array-valued path, index tokenizer is applied';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAnyTokens(data.key, ['quick()brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAnyTokens(data.key, ['quick()brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAllTokens(data.key, ['quick()brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasAllTokens(data.key, ['quick()brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasPhrase(data.key, ['quick()brown']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_separator WHERE hasPhrase(data.key, ['quick()brown']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(r) FROM (SELECT hasPhrase(data.key, ['quick()brown']) AS r FROM tab_array_path_separator ORDER BY id);

SELECT 'array-valued path, index postprocessor is applied';
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAnyTokens(data.key, ['cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAnyTokens(data.key, ['cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAllTokens(data.key, ['cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasAllTokens(data.key, ['cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasPhrase(data.key, ['cat']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_array_path_postprocessor WHERE hasPhrase(data.key, ['cat']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(r) FROM (SELECT hasAnyTokens(data.key, ['cat']) AS r FROM tab_array_path_postprocessor ORDER BY id);

SELECT 'preprocessor, array needles';
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, ['QUICK', 'BROWN']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAnyTokens(data.key, ['QUICK']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAnyTokens(data.key, ['QUICK']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAllTokens(data.key, ['QUICK', 'BROWN']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasAllTokens(data.key, ['QUICK', 'BROWN']) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, 'QUICK BROWN') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_preprocessor WHERE hasPhrase(data.key, 'QUICK BROWN') ORDER BY id) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_split_by_string;
DROP TABLE tab_split_by_non_alpha;
DROP TABLE tab_array;
DROP TABLE tab_ngrams;
DROP TABLE tab_typed;
DROP TABLE tab_array_path_array;
DROP TABLE tab_array_path_split_by_string;
DROP TABLE tab_array_path_separator;
DROP TABLE tab_array_path_postprocessor;
DROP TABLE tab_preprocessor;
