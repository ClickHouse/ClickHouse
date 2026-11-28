-- A null (or, with `input_format_json_empty_as_default`, an empty string) read with `input_format_null_as_default`
-- for a non-Nullable element of an Array, Tuple or Map must store the default of the element type, not the zero of the column.

SELECT 'JSONEachRow Array', CAST(a, 'Array(Int8)') FROM format(JSONEachRow, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '{"a": [null, "b"]}');
SELECT 'JSONCompactEachRow Array', CAST(a, 'Array(Int8)') FROM format(JSONCompactEachRow, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '[[null, "b"]]');
SELECT 'JSONEachRow Tuple object', CAST(t, 'Tuple(UInt8, Int8)') FROM format(JSONEachRow, 't Tuple(n UInt8, e Enum8(''a'' = 1, ''b'' = 2))', '{"t": {"n": 1, "e": null}}');
SELECT 'JSONEachRow Tuple array', CAST(t, 'Tuple(UInt8, Int8)') FROM format(JSONEachRow, 't Tuple(UInt8, Enum8(''a'' = 1, ''b'' = 2))', '{"t": [1, null]}');
SELECT 'JSONEachRow Map', CAST(m, 'Map(String, Int8)') FROM format(JSONEachRow, 'm Map(String, Enum8(''a'' = 1, ''b'' = 2))', '{"m": {"k": null}}');
SELECT 'Values Array', CAST(a, 'Array(Int8)') FROM format(Values, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '([NULL, ''b''])');
SELECT 'Values Array no expressions', CAST(a, 'Array(Int8)') FROM format(Values, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '([NULL, ''b''])') SETTINGS input_format_values_interpret_expressions = 0;
SELECT 'Values Tuple', CAST(t, 'Tuple(Int8)') FROM format(Values, 't Tuple(e Enum8(''a'' = 1, ''b'' = 2))', '((NULL))');
SELECT 'Values Map value', CAST(m, 'Map(String, Int8)') FROM format(Values, 'm Map(String, Enum8(''a'' = 1, ''b'' = 2))', '({''k'': NULL})');
SELECT 'Values Map key', CAST(m, 'Map(Int8, UInt8)') FROM format(Values, 'm Map(Enum8(''a'' = 1, ''b'' = 2), UInt8)', '({NULL: 1})');
SELECT 'TSV Array', CAST(a, 'Array(Int8)') FROM format(TSV, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '[NULL,''b'']');
SELECT 'CSV Array', CAST(a, 'Array(Int8)') FROM format(CSV, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '"[NULL,''b'']"');
SELECT 'CSV Array nested CSV', CAST(a, 'Array(Int8)') FROM format(CSV, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '"[\\N,""b""]"') SETTINGS input_format_csv_arrays_as_nested_csv = 1;
SELECT 'CSV Tuple columns', CAST(t, 'Tuple(UInt8, Int8)') FROM format(CSV, 't Tuple(n UInt8, e Enum8(''a'' = 1, ''b'' = 2))', '1,\\N');
SELECT 'CAST String', CAST(CAST('[NULL, ''b'']' AS Array(Enum8('a' = 1, 'b' = 2))), 'Array(Int8)');
SELECT 'CAST String Variant', CAST(variantElement(CAST('[NULL, ''b'']' AS Variant(Array(Enum8('a' = 1, 'b' = 2)), UInt64)), 'Array(Enum8(''a'' = 1, ''b'' = 2))'), 'Array(Int8)');
SELECT 'Array(Array)', CAST(a, 'Array(Array(Int8))') FROM format(JSONEachRow, 'a Array(Array(Enum8(''a'' = 1, ''b'' = 2)))', '{"a": [[null, "b"]]}');
SELECT 'Array(Tuple)', CAST(a, 'Array(Tuple(UInt8, Int8))') FROM format(JSONEachRow, 'a Array(Tuple(n UInt8, e Enum8(''a'' = 1, ''b'' = 2)))', '{"a": [null, {"n": 1, "e": null}]}');
SELECT 'Map(String, Tuple)', CAST(m, 'Map(String, Tuple(Int8))') FROM format(JSONEachRow, 'm Map(String, Tuple(e Enum8(''a'' = 1, ''b'' = 2)))', '{"m": {"k": null}}');
SELECT 'Array(JSON)', arrayMap(x -> toInt8(x.e), a) FROM format(JSONEachRow, 'a Array(JSON(e Enum8(''a'' = 1, ''b'' = 2)))', '{"a": [null]}');
SELECT 'Enum16', CAST(a, 'Array(Int16)') FROM format(JSONEachRow, 'a Array(Enum16(''x'' = -5, ''y'' = 7))', '{"a": [null, "y"]}');

SELECT 'empty_as_default Array', CAST(a, 'Array(Int8)') FROM format(JSONEachRow, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '{"a": ["", "b"]}') SETTINGS input_format_json_empty_as_default = 1;
SELECT 'empty_as_default Tuple', CAST(t, 'Tuple(Int8)') FROM format(JSONEachRow, 't Tuple(e Enum8(''a'' = 1, ''b'' = 2))', '{"t": {"e": ""}}') SETTINGS input_format_json_empty_as_default = 1;
SELECT 'empty_as_default Map', CAST(m, 'Map(String, Int8)') FROM format(JSONEachRow, 'm Map(String, Enum8(''a'' = 1, ''b'' = 2))', '{"m": {"k": ""}}') SETTINGS input_format_json_empty_as_default = 1;

-- Unchanged: a Nullable element keeps the NULL, a trivial default stays the zero, and without the setting a NULL is rejected.
SELECT 'Array(Nullable)', a FROM format(JSONEachRow, 'a Array(Nullable(Enum8(''a'' = 1, ''b'' = 2)))', '{"a": [null, "b"]}');
SELECT 'Array(UInt8)', a FROM format(JSONEachRow, 'a Array(UInt8)', '{"a": [null, 2]}');
SELECT a FROM format(JSONEachRow, 'a Array(Enum8(''a'' = 1, ''b'' = 2))', '{"a": [null]}') SETTINGS input_format_null_as_default = 0; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE IF EXISTS t_nested_enum_null;
CREATE TABLE t_nested_enum_null (id UInt8, a Array(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nested_enum_null VALUES (1, [NULL, 'b']);
INSERT INTO t_nested_enum_null SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (2, [NULL, 2]);
INSERT INTO t_nested_enum_null FORMAT JSONEachRow {"id": 3, "a": [null, "b"]}

SELECT id, a FROM t_nested_enum_null ORDER BY id;
DROP TABLE t_nested_enum_null;
