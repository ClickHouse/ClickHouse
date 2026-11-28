-- `output_format_native_write_json_as_string` writes a `JSON` stored in the shared variant of a `Dynamic` as text, like one in a regular variant.
SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05817.native', Native)
SELECT arrayJoin(['{"a":1}'::JSON::Dynamic(max_types=0), 42::Int64::Dynamic(max_types=0)]) AS d, [d] AS arr,
    '{"p":[{"b":1}]}'::JSON(max_dynamic_types=0).p AS p
SETTINGS output_format_native_write_json_as_string = 1;

SELECT countSubstrings(raw, '{"a":1}'), countSubstrings(raw, '{"b":1}') FROM file(currentDatabase() || '_05817.native', RawBLOB, 'raw String');
SELECT d, dynamicType(d), isDynamicElementInSharedData(d), arr, p, dynamicType(p)
FROM file(currentDatabase() || '_05817.native', Native)
SETTINGS output_format_native_write_json_as_string = 1;

-- Without the setting, the shared variant is written as is.
INSERT INTO FUNCTION file(currentDatabase() || '_05817.native', Native)
SELECT arrayJoin(['{"a":1}'::JSON::Dynamic(max_types=0), 42::Int64::Dynamic(max_types=0)]) AS d
SETTINGS output_format_native_write_json_as_string = 0;

SELECT countSubstrings(raw, '{"a":1}') FROM file(currentDatabase() || '_05817.native', RawBLOB, 'raw String');
SELECT d, dynamicType(d), isDynamicElementInSharedData(d) FROM file(currentDatabase() || '_05817.native', Native);
