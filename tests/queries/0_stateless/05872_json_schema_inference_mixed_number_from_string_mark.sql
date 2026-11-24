-- A bare JSON number must not be inferred as String because a number parsed from a JSON string was merged with it.

SET input_format_json_try_infer_numbers_from_strings = 1;
SET input_format_json_read_numbers_as_strings = 0;

SELECT x, toTypeName(x) FROM format(JSONEachRow, '{"x":[["1", 18446744073709551615], ["abc"]]}');
SELECT x, toTypeName(x) FROM format(JSONEachRow, '{"x":[[18446744073709551615, "1"], ["abc"]]}');
DESC format(JSONEachRow, '{"x":"1"}\n{"x":18446744073709551615}\n{"x":"abc"}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"x":18446744073709551615}\n{"x":"1"}\n{"x":"abc"}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"x":1.5}\n{"x":"1"}\n{"x":"abc"}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Numbers that were all parsed from strings are still converted back to String.
DESC format(JSONEachRow, '{"x":"1"}\n{"x":"18446744073709551615"}\n{"x":"abc"}');
DESC format(JSONEachRow, '{"x":"1"}\n{"x":"1.5"}\n{"x":"abc"}');
SELECT x, toTypeName(x) FROM format(JSONEachRow, '{"x":[["18446744073709551615", "1"], ["abc"]]}');
