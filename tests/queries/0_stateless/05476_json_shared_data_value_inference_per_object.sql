-- The type of a value in shared data is inferred for every value on its own: the types inferred for a nested JSON
-- must not change the types inferred for the same path in other values, in the next rows or in the same row.
SET max_dynamic_subcolumns_in_json_type_parsing = 1, input_format_try_infer_dates = 1;

SELECT j, JSONAllPathsWithTypes(arrayElement(j.e[], 1))
FROM (SELECT CAST(s, 'JSON') AS j FROM values('s String', '{"a" : 0, "e" : [{"x" : "hello"}]}', '{"a" : 0, "e" : [{"x" : "2024/01/02"}]}'));

SELECT j, JSONAllPathsWithTypes(arrayElement(j.e[], 1))
FROM (SELECT CAST(s, 'JSON') AS j FROM values('s String', '{"a" : 0, "e" : [{"x" : "2024/01/02"}]}', '{"a" : 0, "e" : [{"x" : "hello"}]}'));

SELECT j, JSONAllPathsWithTypes(arrayElement(j.e1[], 1)), JSONAllPathsWithTypes(arrayElement(j.e2[], 1))
FROM (SELECT CAST('{"a" : 0, "e1" : [{"x" : "hello"}], "e2" : [{"x" : "2024/01/02"}]}', 'JSON') AS j);
