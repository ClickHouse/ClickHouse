-- A type freed during inference must not pass its negative or parsed-from-string mark to another value.

DESC format(JSONEachRow, '{"x":0}\n{"x":-1}\n{"y":1}\n{"y":18446744073709551615}');
DESC format(JSONEachRow, '{"x":-1}\n{"x":-2}\n{"y":1}\n{"y":18446744073709551615}');
DESC format(JSONEachRow, '{"a":[-1, 1.5], "b":[1, 18446744073709551615]}');
DESC format(JSONEachRow, '{"x":"1"}\n{"x":"2"}\n{"y":1}\n{"y":"abc"}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1, input_format_json_read_numbers_as_strings = 0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- The marks still work.
DESC format(JSONEachRow, '{"x":0}\n{"x":-1}\n{"y":-1}\n{"y":18446744073709551615}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"x":["-1"]}\n{"x":["18446744073709551615"]}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"x":"1"}\n{"x":1.5}\n{"x":"abc"}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1, input_format_json_read_numbers_as_strings = 0;
DESC format(JSONEachRow, '{"x":"-1"}\n{"x":"abc"}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1, input_format_json_read_numbers_as_strings = 0;
DESC format(JSONEachRow, '{"x":1}\n{"x":1.5}\n{"x":"abc"}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1, input_format_json_read_numbers_as_strings = 0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"x":"1"}\n{"x":"18446744073709551615"}\n{"x":"abc"}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1, input_format_json_read_numbers_as_strings = 0;
SELECT dynamicType(j.a), dynamicType(j.b), dynamicType(j.c) FROM format(JSONEachRow, 'j JSON', '{"j":{"a":[-1, 1], "b":["-1", "18446744073709551615"], "c":[-1, 18446744073709551615]}}') SETTINGS input_format_json_try_infer_numbers_from_strings = 1;
