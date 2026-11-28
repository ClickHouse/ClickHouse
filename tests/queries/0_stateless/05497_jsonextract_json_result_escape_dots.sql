SET json_type_escape_dots_in_keys = 1;

-- 1. Dotted-key escaping: JSONExtract into JSON must match CAST and keep both paths.
SELECT toString(JSONExtract('{"a.b":2,"a":{"b":3}}', 'JSON')) = toString(CAST('{"a.b":2,"a":{"b":3}}', 'JSON'));
SELECT arraySort(JSONAllPaths(JSONExtract('{"a.b":2,"a":{"b":3}}', 'JSON')));

-- 2. JSONExtractKeysAndValues must keep both paths and values of the value object.
SELECT arraySort(JSONAllPaths(v)),
       JSONExtractInt(toString(v), 'x.y'),
       JSONExtractInt(toString(v), 'x', 'y')
FROM (SELECT arrayElement(JSONExtractKeysAndValues('{"a":{"x.y":1,"x":{"y":2}}}', 'JSON'), 1).2 AS v);

-- 3. Typed-path skipping must use the per-query setting, like CAST.
SET type_json_skip_invalid_typed_paths = 1;
SELECT toString(JSONExtract('{"a":"not_an_int","b":"valid","c":123}', 'JSON(a Int64, b String, c Int32)'))
     = toString(CAST('{"a":"not_an_int","b":"valid","c":123}', 'JSON(a Int64, b String, c Int32)'));

-- 4. SKIP REGEXP: full match keeps skip_abc, partial match removes it.
SET type_json_use_partial_match_to_skip_paths_by_regexp = 0;
SELECT arraySort(JSONAllPaths(JSONExtract('{"skip_abc":42}', 'JSON(SKIP REGEXP ''skip'')')));
SET type_json_use_partial_match_to_skip_paths_by_regexp = 1;
SELECT arraySort(JSONAllPaths(JSONExtract('{"skip_abc":42}', 'JSON(SKIP REGEXP ''skip'')')));
