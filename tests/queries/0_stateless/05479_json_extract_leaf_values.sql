-- Tests for JSONExtractLeafValues and JSONExtractLeafPathsAndValues.

SELECT '-- nested objects, scalars, null skipping, empty containers';
SELECT JSONExtractLeafValues('{"a": {"b": "x", "c": {"d": 1}}, "e": null, "f": {}, "g": []}');
SELECT JSONExtractLeafPathsAndValues('{"a": {"b": "x", "c": {"d": 1}}, "e": null, "f": {}, "g": []}');

SELECT '-- strings are unescaped, numbers and booleans are JSON text';
SELECT JSONExtractLeafValues('{"s": "a\\"b\\\\c\\nd\\u00e9", "i": -5, "u": 18446744073709551615, "f": 1.50, "e": 1e2, "t": true, "n": false, "z": ""}');
SELECT JSONExtractLeafPathsAndValues('{"f": 1.50, "t": true}').2;
SELECT JSONExtractRaw('{"f": 1.50}', 'f'), JSONExtractLeafValues('{"f": 1.50}');

SELECT '-- arrays: elements (default)';
SELECT JSONExtractLeafPathsAndValues('{"a": [1, "x", null, true, {"b": 2, "c": {"d": 3}}, [4, [5]], {}, []], "e": 6}');
SELECT JSONExtractLeafPathsAndValues('{"a": [1, {"b": 2}]}', '', 'elements') = JSONExtractLeafPathsAndValues('{"a": [1, {"b": 2}]}');
SELECT '-- arrays: skip';
SELECT JSONExtractLeafPathsAndValues('{"a": [1, {"b": 2}], "c": "x", "d": {"e": [3]}}', '', 'skip');
SELECT '-- arrays: string';
SELECT JSONExtractLeafPathsAndValues('{"a": [1, "x", null, {"b": 2.50}, [3]], "c": "y", "d": [], "e": {"f": []}}', '', 'string');
SELECT JSONExtractLeafValues('{"a": [], "b": [[]], "c": {}}', '', 'elements');
SELECT JSONExtractLeafValues('{"a": [], "b": [[]], "c": {}}', '', 'string');

SELECT '-- top level is not an object, or the JSON is invalid';
SELECT JSONExtractLeafValues('[1, 2]'), JSONExtractLeafValues('"x"'), JSONExtractLeafValues('1'), JSONExtractLeafValues('null'), JSONExtractLeafValues('{"a": '), JSONExtractLeafValues(''), JSONExtractLeafValues('{}');
SELECT JSONExtractLeafPathsAndValues('[1, 2]'), JSONExtractLeafPathsAndValues('{"a"'), JSONExtractLeafPathsAndValues('{}');
SELECT toTypeName(JSONExtractLeafValues('{}')), toTypeName(JSONExtractLeafPathsAndValues('{}'));

SELECT '-- duplicate keys are all emitted in document order';
SELECT JSONExtractLeafPathsAndValues('{"a": 1, "b": 2, "a": 3, "a": {"c": 4}}');

SELECT '-- empty key';
SELECT JSONExtractLeafPathsAndValues('{"": 1, "a": {"": 2}, "": {"b": 3}}');

SELECT '-- skip_regex: empty regex skips nothing';
SELECT JSONExtractLeafValues('{"a": 1, "b_noindex": 2}', '');
SELECT '-- skip_regex: subtrees, arrays, members inside arrays, element paths';
SELECT JSONExtractLeafPathsAndValues('{"a": 1, "b_noindex": {"c": 2, "d": [3]}, "e": {"f_noindex": 4, "g": 5}}', '.*_noindex$');
SELECT JSONExtractLeafPathsAndValues('{"a": [{"x_noindex": 1, "y": 2}, {"x": 3}], "b_noindex": [4, 5], "c": [6, 7]}', '.*_noindex$');
SELECT JSONExtractLeafPathsAndValues('{"a": [10, 11, [12, 13]], "b": 14}', '^a\\[1\\]$');
SELECT JSONExtractLeafPathsAndValues('{"a": [10, 11, [12, 13]], "b": 14}', '^a\\[2\\]');
SELECT JSONExtractLeafPathsAndValues('{"a": [10, 11, [12, 13]], "b": 14}', '^a\\[2\\]\\[0\\]$');
SELECT JSONExtractLeafPathsAndValues('{"a": [10, 11], "b": {"c": 1}}', '^a$', 'string');
SELECT JSONExtractLeafPathsAndValues('{"a": [10, 11], "b": {"c": 1}}', '^b\\.c$', 'string');

SELECT '-- a partial match is enough, type_json_use_partial_match_to_skip_paths_by_regexp does not apply';
SELECT JSONExtractLeafValues('{"abc": 1, "b": 2, "xbx": 3}', 'b') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1;
SELECT JSONExtractLeafValues('{"abc": 1, "b": 2, "xbx": 3}', 'b') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;
SELECT JSONExtractLeafValues('{"abc": 1, "b": 2, "xbx": 3}', '.*b.*') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;
SELECT JSONExtractLeafValues('{"abc": 1, "b": 2, "xbx": 3}', '^b$') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1;
SELECT JSONExtractLeafValues('{"abc": 1, "b": 2, "xbx": 3}', '^b$') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;

SELECT '-- an array that cannot be rendered gives an empty array for the row';
SELECT JSONExtractLeafValues(concat('{"a": ', repeat('[', 20), repeat(']', 20), ', "b": 1}'), '', 'string') SETTINGS input_format_json_max_depth = 10;
SELECT JSONExtractLeafPathsAndValues(concat('{"b": 1, "a": ', repeat('[', 20), repeat(']', 20), '}'), '', 'string') SETTINGS input_format_json_max_depth = 10;
SELECT JSONExtractLeafPathsAndValues(s, '', 'string') FROM (SELECT arrayJoin(['{"x": 1}', concat('{"b": 1, "a": ', repeat('[', 20), repeat(']', 20), '}'), '{"y": [2]}']) AS s) SETTINGS input_format_json_max_depth = 10;
SELECT JSONExtractLeafValues(concat('{"a": ', repeat('[', 20), '1', repeat(']', 20), ', "b": 1}'), '', 'elements') SETTINGS input_format_json_max_depth = 10;

SELECT '-- dots in keys: json_type_escape_dots_in_keys on and off';
SELECT JSONExtractLeafPathsAndValues('{"a.b": 1, "a": {"b": 2, "c.d": {"e.f": 3}}, "g": [{"h.i": 4}]}') SETTINGS json_type_escape_dots_in_keys = 1;
SELECT JSONExtractLeafPathsAndValues('{"a.b": 1, "a": {"b": 2, "c.d": {"e.f": 3}}, "g": [{"h.i": 4}]}') SETTINGS json_type_escape_dots_in_keys = 0;
SELECT '-- the regex is matched against the escaped path';
SELECT JSONExtractLeafPathsAndValues('{"a.b": 1, "a": {"b": 2}}', '^a%2Eb$') SETTINGS json_type_escape_dots_in_keys = 1;
SELECT JSONExtractLeafPathsAndValues('{"a.b": 1, "a": {"b": 2}}', '^a\\.b$') SETTINGS json_type_escape_dots_in_keys = 1;
SELECT JSONExtractLeafPathsAndValues('{"a.b": 1, "a": {"b": 2}}', '^a\\.b$') SETTINGS json_type_escape_dots_in_keys = 0;
SELECT '-- no other character is escaped';
SELECT JSONExtractLeafPathsAndValues('{"a%b": 1, "c[0]": 2, "d e": 3}') SETTINGS json_type_escape_dots_in_keys = 1;

SELECT '-- an anchored regex gives the same result with all setting combinations';
WITH '{"a": 1, "b_noindex": {"c": 2}, "d": [{"e_noindex": 3, "f": 4}, 5], "g.h": 6, "i.j_noindex": 7}' AS j
SELECT
    JSONExtractLeafValues(j, '.*_noindex$') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1, json_type_escape_dots_in_keys = 1;
WITH '{"a": 1, "b_noindex": {"c": 2}, "d": [{"e_noindex": 3, "f": 4}, 5], "g.h": 6, "i.j_noindex": 7}' AS j
SELECT
    JSONExtractLeafValues(j, '.*_noindex$') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0, json_type_escape_dots_in_keys = 0;
WITH '{"a": 1, "b_noindex": {"c": 2}, "d": [{"e_noindex": 3, "f": 4}, 5], "g.h": 6, "i.j_noindex": 7}' AS j
SELECT
    JSONExtractLeafValues(j, '.*_noindex$') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1, json_type_escape_dots_in_keys = 0;
WITH '{"a": 1, "b_noindex": {"c": 2}, "d": [{"e_noindex": 3, "f": 4}, 5], "g.h": 6, "i.j_noindex": 7}' AS j
SELECT
    JSONExtractLeafValues(j, '.*_noindex$') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0, json_type_escape_dots_in_keys = 1;

SELECT '-- table column, constant options, Nullable and LowCardinality input';
SELECT JSONExtractLeafValues(s, 'b', 'skip') FROM (SELECT arrayJoin(['{"a": 1, "b": 2, "c": [3]}', '{"x": {"y": "z"}}', 'bad', '[]']) AS s);
SELECT JSONExtractLeafValues(s) FROM (SELECT arrayJoin(['{"a": 1}', NULL, '{"b": "x"}']::Array(Nullable(String))) AS s);
SELECT toTypeName(JSONExtractLeafValues(CAST('{"a": 1}' AS Nullable(String)))), JSONExtractLeafValues(CAST(NULL AS Nullable(String)));
SELECT JSONExtractLeafValues(NULL), JSONExtractLeafPathsAndValues(NULL, '');
SELECT JSONExtractLeafValues(toLowCardinality('{"a": 1, "b": {"c": 2}}'), 'c');
SELECT JSONExtractLeafValues(materialize('{"a": 1, "b": {"c": 2}}'), 'c', 'string');

SELECT '-- errors';
SELECT JSONExtractLeafValues(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT JSONExtractLeafValues('{}', '', 'elements', 'x'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT JSONExtractLeafValues('{}', '(', 'skip'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT JSONExtractLeafValues('{}', '', 'Skip'); -- { serverError BAD_ARGUMENTS }
SELECT JSONExtractLeafValues('{}', '', 'ELEMENTS'); -- { serverError BAD_ARGUMENTS }
SELECT JSONExtractLeafValues('{}', '', ''); -- { serverError BAD_ARGUMENTS }
SELECT JSONExtractLeafPathsAndValues('{}', '', 'all'); -- { serverError BAD_ARGUMENTS }
SELECT JSONExtractLeafValues('{}', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractLeafValues('{}', '', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractLeafValues('{}', NULL); -- { serverError ILLEGAL_COLUMN }
SELECT JSONExtractLeafValues('{}', '', NULL); -- { serverError ILLEGAL_COLUMN }
SELECT JSONExtractLeafValues(s, materialize('a')) FROM (SELECT '{}' AS s); -- { serverError ILLEGAL_COLUMN }
SELECT JSONExtractLeafValues(s, '', materialize('skip')) FROM (SELECT '{}' AS s); -- { serverError ILLEGAL_COLUMN }
SELECT JSONExtractLeafValues(s, CAST(NULL AS Nullable(String))) FROM (SELECT '{}' AS s); -- { serverError ILLEGAL_COLUMN }
SELECT JSONExtractLeafValues(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractLeafValues(toFixedString('{"a": 1}', 8)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractLeafPathsAndValues('{"a": 1}'::JSON); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractLeafValues('{"a": 1}'::JSON, '.*'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- text index over JSONExtractLeafValues with a skip regex';
DROP TABLE IF EXISTS tab_leaf;
CREATE TABLE tab_leaf
(
    id UInt64,
    s String,
    INDEX idx JSONExtractLeafValues(s, '.*_noindex$') TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_leaf SELECT number, format('{{"msg": "{}", "attrs": {{"user": "user{}", "secret_noindex": "hidden{}", "list": [{{"tag": "tag{}"}}, "item{}"]}}}}',
    ['alpha', 'beta', 'gamma', 'delta'][intDiv(number, 4) % 4 + 1], number % 4, intDiv(number, 4), number % 4, intDiv(number, 4)) FROM numbers(16);

SELECT s FROM tab_leaf WHERE id = 5;
SELECT count() FROM tab_leaf WHERE hasAllTokens(JSONExtractLeafValues(s, '.*_noindex$'), 'beta');
SELECT count() FROM tab_leaf WHERE hasAllTokens(JSONExtractLeafValues(s, '.*_noindex$'), 'hidden1');
SELECT count() FROM tab_leaf WHERE hasAllTokens(JSONExtractLeafValues(s, ''), 'hidden1');
SELECT count() FROM tab_leaf WHERE hasAllTokens(JSONExtractLeafValues(s, '.*_noindex$'), ['beta', 'item1']);

SELECT 'index is used';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_leaf WHERE hasAllTokens(JSONExtractLeafValues(s, '.*_noindex$'), 'beta')
    SETTINGS query_plan_optimize_count_from_text_index = 0, use_skip_indexes = 1
) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'a different regex does not use the index';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM tab_leaf WHERE hasAllTokens(JSONExtractLeafValues(s, ''), 'beta')
    SETTINGS query_plan_optimize_count_from_text_index = 0, use_skip_indexes = 1
) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab_leaf;

SELECT '-- an unanchored regex in an index gives the same result when the settings of insert and query differ';
DROP TABLE IF EXISTS tab_leaf_settings;
CREATE TABLE tab_leaf_settings
(
    id UInt64,
    s String,
    INDEX idx JSONExtractLeafValues(s, 'noindex') TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_leaf_settings SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1
    VALUES (1, '{"a": "hello", "x_noindex": "secret"}'), (2, '{"a": "secret", "noindex": {"k": "zzz"}}');
INSERT INTO tab_leaf_settings SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0
    VALUES (3, '{"a": "secret", "y_noindex": "secret"}');
OPTIMIZE TABLE tab_leaf_settings FINAL SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;

SELECT groupArray(id) FROM tab_leaf_settings WHERE hasAllTokens(JSONExtractLeafValues(s, 'noindex'), 'secret') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;
SELECT groupArray(id) FROM tab_leaf_settings WHERE hasAllTokens(JSONExtractLeafValues(s, 'noindex'), 'secret') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0, use_skip_indexes = 0;
SELECT count() FROM tab_leaf_settings WHERE hasAllTokens(JSONExtractLeafValues(s, 'noindex'), 'secret') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;
SELECT count() FROM tab_leaf_settings WHERE hasAllTokens(JSONExtractLeafValues(s, 'noindex'), 'secret') SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1;

DROP TABLE tab_leaf_settings;
