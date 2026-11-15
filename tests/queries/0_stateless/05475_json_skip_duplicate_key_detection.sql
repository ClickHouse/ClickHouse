-- Tests for type_json_skip_duplicate_key_detection: parsing is correct when keys are unique and stays safe
-- (no corruption, errors still raised or duplicates still skipped) when a duplicated key is actually present.

-- Unique keys: the result is identical with detection on and off.
SELECT CAST('{"a" : 1, "b" : "x", "c" : [1, 2, 3], "d" : {"e" : 5}}', 'JSON') AS j SETTINGS type_json_skip_duplicate_key_detection = 0;
SELECT CAST('{"a" : 1, "b" : "x", "c" : [1, 2, 3], "d" : {"e" : 5}}', 'JSON') AS j SETTINGS type_json_skip_duplicate_key_detection = 1;

-- Same shape forced into shared data (nested objects get max_dynamic_paths = 0): detection on must match off.
SELECT CAST('{"obj" : [{"k1" : 1, "k2" : "x", "k3" : ["1.2"]}, {"k1" : 2, "k2" : "y", "k3" : ["3.4"]}]}', 'JSON') AS j SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 0, type_json_skip_duplicate_key_detection = 0;
SELECT CAST('{"obj" : [{"k1" : 1, "k2" : "x", "k3" : ["1.2"]}, {"k1" : 2, "k2" : "y", "k3" : ["3.4"]}]}', 'JSON') AS j SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 0, type_json_skip_duplicate_key_detection = 1;

-- Real duplicate, same path, strict: an exception is still raised with detection on (the per-path check downstream catches it).
SELECT CAST('{"a" : 1, "a" : 2}', 'JSON') AS j SETTINGS type_json_skip_duplicate_key_detection = 1, type_json_allow_duplicated_key_with_literal_and_nested_object = 0; -- { serverError INCORRECT_DATA }

-- Real duplicate, same path, with type_json_skip_duplicated_paths: the first value is kept, no corruption.
SELECT CAST('{"a" : 1, "a" : 2}', 'JSON') AS j SETTINGS type_json_skip_duplicate_key_detection = 1, type_json_skip_duplicated_paths = 1;
SELECT CAST('{"a" : 1, "a" : 2, "a" : 3}', 'JSON') AS j SETTINGS type_json_skip_duplicate_key_detection = 1, type_json_skip_duplicated_paths = 1;

-- Documented behavior difference: a literal+object duplicate that detection would reject (allow = 0) is instead
-- accepted and the nested object's fields are kept as distinct paths. No corruption, no exception.
SELECT CAST('{"a" : 1, "a" : {"b" : 2}}', 'JSON') AS j SETTINGS type_json_skip_duplicate_key_detection = 1, type_json_allow_duplicated_key_with_literal_and_nested_object = 0;
SELECT CAST('{"a" : {"b" : 1}, "a" : {"c" : 2}}', 'JSON') AS j SETTINGS type_json_skip_duplicate_key_detection = 1, type_json_allow_duplicated_key_with_literal_and_nested_object = 0;

-- Column stays consistent after inserting many rows, some with duplicated keys skipped: all rows present, subcolumn readable.
DROP TABLE IF EXISTS t_json_skip_dup;
CREATE TABLE t_json_skip_dup (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_skip_dup SELECT number, '{"a" : 1, "a" : 2, "b" : "x"}' FROM numbers(1000)
SETTINGS type_json_skip_duplicate_key_detection = 1, type_json_skip_duplicated_paths = 1;
SELECT count(), countDistinct(j.a), countDistinct(j.b) FROM t_json_skip_dup;
DROP TABLE t_json_skip_dup;
