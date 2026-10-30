-- Direct read from a text index computes the predicate from its expression on parts without the index.
-- A lambda that uses no column is folded into a constant, which was written out as the tuple of its captured
-- values, so `arrayMap` got a tuple instead of a function.

SET enable_full_text_index = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

SELECT 'index added after an insert';
CREATE TABLE tab (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('alpha zebra');
ALTER TABLE tab ADD INDEX idx arrayMap(x -> concat('p', x), splitByChar(' ', s)) TYPE text(tokenizer = 'array');
INSERT INTO tab VALUES ('beta zebra');
SELECT count() FROM tab WHERE hasAllTokens(arrayMap(x -> concat('p', x), splitByChar(' ', s)), ['pzebra']);
SELECT count() FROM tab WHERE hasAllTokens(arrayMap(x -> concat('p', x), splitByChar(' ', s)), ['pzebra']) SETTINGS query_plan_direct_read_from_text_index = 0;
DROP TABLE tab;

SELECT 'index cleared in a partition';
CREATE TABLE tab (p UInt8, s String, INDEX idx arrayMap(x -> concat('p', x), splitByChar(' ', s)) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO tab VALUES (1, 'alpha zebra');
INSERT INTO tab VALUES (2, 'beta zebra');
ALTER TABLE tab CLEAR INDEX idx IN PARTITION 1 SETTINGS mutations_sync = 2;
SELECT count() FROM tab WHERE hasAllTokens(arrayMap(x -> concat('p', x), splitByChar(' ', s)), ['pzebra']);
DROP TABLE tab;

SELECT 'other lambdas';
CREATE TABLE tab (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('alpha zebra'), ('gamma');
ALTER TABLE tab ADD INDEX idx_no_captures arrayMap(x -> lower(x), splitByChar(' ', s)) TYPE text(tokenizer = 'array');
ALTER TABLE tab ADD INDEX idx_filter arrayFilter(x -> length(x) > 4, splitByChar(' ', s)) TYPE text(tokenizer = 'array');
ALTER TABLE tab ADD INDEX idx_nested_constant arrayMap(x -> arrayStringConcat(arrayMap(y -> concat('p', y), [x])), splitByChar(' ', s)) TYPE text(tokenizer = 'array');
ALTER TABLE tab ADD INDEX idx_nested_argument arrayMap(x -> concat(arrayStringConcat(arrayMap(y -> concat('p', x, y), ['q'])), toString(length(s))), splitByChar(' ', s)) TYPE text(tokenizer = 'array');
ALTER TABLE tab ADD INDEX idx_column arrayMap(x -> concat(x, upper(substring(s, 1, 1))), splitByChar(' ', s)) TYPE text(tokenizer = 'array');
INSERT INTO tab VALUES ('beta zebra'), ('delta');

-- No captured values at all.
SELECT count() FROM tab WHERE hasAllTokens(arrayMap(x -> lower(x), splitByChar(' ', s)), ['zebra']);
SELECT count() FROM tab WHERE hasAllTokens(arrayFilter(x -> length(x) > 4, splitByChar(' ', s)), ['zebra']);
-- The inner lambda is folded too, and is captured by the outer one.
SELECT count() FROM tab WHERE hasAllTokens(arrayMap(x -> arrayStringConcat(arrayMap(y -> concat('p', y), [x])), splitByChar(' ', s)), ['pzebra']);
-- The inner lambda uses the argument of the outer one, which uses a column: its captured constant used to stay an unknown identifier.
SELECT count() FROM tab WHERE hasAnyTokens(arrayMap(x -> concat(arrayStringConcat(arrayMap(y -> concat('p', x, y), ['q'])), toString(length(s))), splitByChar(' ', s)), ['pzebraq11', 'pdeltaq5']);
-- A lambda that uses a column is not folded.
SELECT count() FROM tab WHERE hasAnyTokens(arrayMap(x -> concat(x, upper(substring(s, 1, 1))), splitByChar(' ', s)), ['zebraA', 'zebraB']);
DROP TABLE tab;
