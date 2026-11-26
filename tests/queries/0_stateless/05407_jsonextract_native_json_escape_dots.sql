SET json_type_escape_dots_in_keys = 1;

SELECT
    JSONExtractInt('{"a.b":42,"a":{"b":7}}', 'a.b') AS string_literal_dot_key,
    JSONExtractInt('{"a.b":42,"a":{"b":7}}'::JSON, 'a.b') AS json_literal_dot_key,
    JSONExtractInt('{"a.b":42,"a":{"b":7}}'::JSON, 'a', 'b') AS json_nested_key
FORMAT TabSeparated;
