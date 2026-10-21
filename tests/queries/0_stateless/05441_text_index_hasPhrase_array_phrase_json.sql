-- Tests that `hasPhrase` with an `Array(String)` phrase on a JSON path.
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

INSERT INTO tab_split_by_string VALUES (1, '{"key": "the quick brown fox"}'), (2, '{"key": "lazy dog"}'), (3, '{"key": "brown quick"}');
INSERT INTO tab_split_by_non_alpha SELECT * FROM tab_split_by_string;
INSERT INTO tab_array SELECT * FROM tab_split_by_string;
INSERT INTO tab_ngrams SELECT * FROM tab_split_by_string;
INSERT INTO tab_typed SELECT * FROM tab_split_by_string;

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

DROP TABLE tab_split_by_string;
DROP TABLE tab_split_by_non_alpha;
DROP TABLE tab_array;
DROP TABLE tab_ngrams;
DROP TABLE tab_typed;
