-- `parseQueryToJSON` and `formatQueryFromJSON` are documented as a round trip of the query AST
-- (`FunctionDocumentation` of `formatQueryFromJSON`, `src/Functions/formatQueryFromJSON.cpp`).
-- For `CREATE FUNCTION ... LANGUAGE WASM` that means every optional clause has to come back
-- unchanged, and a JSON document that the parser could never have produced has to be rejected at
-- the JSON boundary with `BAD_ARGUMENTS` instead of building a parser-impossible AST.

-- The optional `:: source_function_name`, the `SHA256_HASH` and a multi-element `SETTINGS` list all
-- survive the round trip, and the result is the canonical formatting of the original query.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''))
     = formatQuerySingleLine('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json''');

-- `module_name`, `source_function_name` and `module_hash` are read back as string literals and are
-- later taken apart with `value.safeGet<String>()`, so a `Literal` carrying another field type has
-- to be rejected here instead of reaching that cast.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    '"field_type":"String","value":"mod"',
    '"field_type":"UInt64","value":1')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    '"field_type":"String","value":"fn"',
    '"field_type":"Float64","value":1.5')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    '"field_type":"String","value":"hash"',
    '"field_type":"Bool","value":true')); -- { serverError BAD_ARGUMENTS }

-- `result_type` and `module_name` are mandatory: `ParserCreateFunctionQuery` always fills both, and
-- the formatter dereferences them unconditionally, so a document without them has to be rejected.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    ',"result_type":{"type":"DataType","name":"UInt64"}',
    '')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    ',"module_name":{"type":"Literal","value":{"field_type":"String","value":"mod"}}',
    '')); -- { serverError BAD_ARGUMENTS }

-- Every element of `function_settings` is a `{"name": ..., "value": ...}` object; a null element or
-- an element without a `value` would otherwise become a setting change with an empty `Field`.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    '{"name":"max_fuel","value":{"field_type":"UInt64","value":7}}',
    'null')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    '{"name":"max_fuel","value":{"field_type":"UInt64","value":7}}',
    '{"name":"max_fuel"}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE FUNCTION wf LANGUAGE WASM ARGUMENTS (x UInt32) RETURNS UInt64 FROM ''mod'' :: ''fn'' ABI plain DETERMINISTIC SHA256_HASH ''hash'' SETTINGS max_fuel = 7, serialization_format = ''json'''),
    '{"name":"max_fuel","value":',
    '{"name":7,"value":')); -- { serverError BAD_ARGUMENTS }
