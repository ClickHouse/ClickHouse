-- JSONExtractLeafValues and JSONExtractLeafPathsAndValues can be an index expression, which is computed with the server-level settings.
-- So the result must not depend on the session settings.

SELECT '-- the same result with allow_simdjson = 0 and 1';
SELECT n, JSONExtractLeafValues(concat('{"a": ', n, '}')) FROM (SELECT arrayJoin(['18446744073709551616', '-9223372036854775809', '123456789012345678901234567890', '0.1']) AS n) SETTINGS allow_simdjson = 0;
SELECT n, JSONExtractLeafValues(concat('{"a": ', n, '}')) FROM (SELECT arrayJoin(['18446744073709551616', '-9223372036854775809', '123456789012345678901234567890', '0.1']) AS n) SETTINGS allow_simdjson = 1;
SELECT JSONExtractLeafPathsAndValues('{"a": [18446744073709551616, {"b": 1.50}]}', '', 'string') SETTINGS allow_simdjson = 0;
SELECT JSONExtractLeafPathsAndValues('{"a": [18446744073709551616, {"b": 1.50}]}', '', 'string') SETTINGS allow_simdjson = 1;

SELECT '-- 1023 levels are parsed, 1024 levels are not, with allow_simdjson = 0 and 1';
SELECT length(JSONExtractLeafValues(concat('{"a": ', repeat('[', 1022), '1', repeat(']', 1022), '}'))), length(JSONExtractLeafValues(concat('{"a": ', repeat('[', 1023), '1', repeat(']', 1023), '}'))) SETTINGS allow_simdjson = 0;
SELECT length(JSONExtractLeafValues(concat('{"a": ', repeat('[', 1022), '1', repeat(']', 1022), '}'))), length(JSONExtractLeafValues(concat('{"a": ', repeat('[', 1023), '1', repeat(']', 1023), '}'))) SETTINGS allow_simdjson = 1;

SELECT '-- the string mode has no depth limit';
SELECT length(JSONExtractLeafValues(concat('{"a": ', repeat('[', 200), '1', repeat(']', 200), '}'), '', 'string')[1]) SETTINGS input_format_json_max_depth = 10;
SELECT JSONExtractLeafPathsAndValues(concat('{"a": ', repeat('[', 20), '"x"', repeat(']', 20), ', "b": 1}'), '', 'string') SETTINGS input_format_json_max_depth = 10;

SELECT '-- a text index gives the same result as a full scan with other session settings';
DROP TABLE IF EXISTS tab_leaf_session;
CREATE TABLE tab_leaf_session
(
    id UInt64,
    s String,
    INDEX idx_elements JSONExtractLeafValues(s) TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_string JSONExtractLeafValues(s, '', 'string') TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_leaf_session VALUES (1, '{"n": 18446744073709551616}'), (2, '{"n": 1}'), (3, concat('{"a": ', repeat('[', 20), '"secret"', repeat(']', 20), '}'));

SELECT groupArray(id) FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s), '18446744073709551616') SETTINGS allow_simdjson = 0;
SELECT groupArray(id) FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s), '18446744073709551616') SETTINGS allow_simdjson = 0, use_skip_indexes = 0;
SELECT groupArray(id) FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s), '18446744073709552000') SETTINGS allow_simdjson = 0;
SELECT groupArray(id) FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s), '18446744073709552000') SETTINGS allow_simdjson = 0, use_skip_indexes = 0;
SELECT groupArray(id) FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s, '', 'string'), 'secret') SETTINGS input_format_json_max_depth = 10;
SELECT groupArray(id) FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s, '', 'string'), 'secret') SETTINGS input_format_json_max_depth = 10, use_skip_indexes = 0;
SELECT count() FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s, '', 'string'), 'secret') SETTINGS input_format_json_max_depth = 10, query_plan_optimize_count_from_text_index = 1;
SELECT count() FROM tab_leaf_session WHERE hasAllTokens(JSONExtractLeafValues(s, '', 'string'), 'secret') SETTINGS input_format_json_max_depth = 10, use_skip_indexes = 0;

DROP TABLE tab_leaf_session;

SELECT '-- json_type_escape_dots_in_keys of an insert or a query does not change the result, with and without the index';
DROP TABLE IF EXISTS tab_leaf_dots;
CREATE TABLE tab_leaf_dots
(
    id UInt64,
    s String,
    INDEX idx JSONExtractLeafValues(s, '^a\\.b$') TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_leaf_dots SETTINGS json_type_escape_dots_in_keys = 1
    VALUES (1, '{"a.b": "secret", "c": "x"}'), (2, '{"a": {"b": "secret"}}'), (3, '{"c": "secret"}');
INSERT INTO tab_leaf_dots SETTINGS json_type_escape_dots_in_keys = 0
    VALUES (4, '{"a.b": "secret", "c": "x"}'), (5, '{"c": "secret", "a.c": "hello"}');

SELECT arraySort(groupArray(id)) FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'secret') SETTINGS json_type_escape_dots_in_keys = 0;
SELECT arraySort(groupArray(id)) FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'secret') SETTINGS json_type_escape_dots_in_keys = 0, use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'secret') SETTINGS json_type_escape_dots_in_keys = 1;
SELECT arraySort(groupArray(id)) FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'secret') SETTINGS json_type_escape_dots_in_keys = 1, use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'hello') SETTINGS json_type_escape_dots_in_keys = 1;
SELECT arraySort(groupArray(id)) FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'hello') SETTINGS json_type_escape_dots_in_keys = 1, use_skip_indexes = 0;
SELECT count() FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'secret') SETTINGS json_type_escape_dots_in_keys = 1, query_plan_optimize_count_from_text_index = 1;
SELECT count() FROM tab_leaf_dots WHERE hasAllTokens(JSONExtractLeafValues(s, '^a\\.b$'), 'secret') SETTINGS json_type_escape_dots_in_keys = 1, use_skip_indexes = 0;

DROP TABLE tab_leaf_dots;
