-- An object field whose types cannot be merged is a conflict, as for a top-level column.
SET input_format_json_try_infer_named_tuples_from_objects = 1;
SET input_format_try_infer_variants = 0;
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types = 1;

SELECT * FROM format(JSONEachRow, '{"t":{"k":-1}}\n{"t":{"k":18446744073709551615}}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM format(JSONEachRow, '{"t":{"k":18446744073709551615}}\n{"t":{"k":-1}}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM format(JSONEachRow, '{"t":{"a":{"k":-5}}}\n{"t":{"a":{"k":18446744073709551000}}}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM format(JSONEachRow, '{"a":[{"k":-1}]}\n{"a":[{"k":18446744073709551615}]}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM format(JSONColumns, '{"t":[{"k":-1},{"k":18446744073709551615}]}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM format(JSONEachRow, '{"t":{"k":"abc"}}\n{"t":{"k":[]}}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Objects with conflicting fields in one array are elements of different types.
SELECT a, toTypeName(a) FROM format(JSONEachRow, '{"a":[{"k":-1},{"k":18446744073709551615}]}');
SELECT x, dynamicType(x) FROM format(JSONEachRow, 'x Dynamic', '{"x":[{"k":-1},{"k":18446744073709551615}]}');

-- Mergeable fields and Variant are unchanged.
SELECT t, toTypeName(t) FROM format(JSONEachRow, '{"t":{"k":1}}\n{"t":{"k":18446744073709551615,"j":"x"}}');
SELECT t, toTypeName(t) FROM format(JSONEachRow, '{"t":{"k":-1}}\n{"t":{"k":18446744073709551615}}') SETTINGS input_format_try_infer_variants = 1;
