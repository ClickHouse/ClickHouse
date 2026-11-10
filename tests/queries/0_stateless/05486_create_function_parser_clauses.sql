-- Clause combinations of CREATE/ATTACH FUNCTION, as accepted by
-- ParserCreateFunctionQuery::parseImpl (src/Parsers/ParserCreateFunctionQuery.cpp).
--
-- Expected behaviour source: ParserCreateFunctionQuery::getDocumentation()
-- ["CREATE FUNCTION"] for the lambda form, plus the syntax comments of
-- parseImpl for the two other forms it accepts: the driver form
--   [ARGUMENTS (...)] [RETURNS T] ENGINE = Driver(k = v, ...) AS '...code...'
-- and the WebAssembly form
--   LANGUAGE WASM ARGUMENTS (...) RETURNS T FROM 'module' [:: 'symbol']
--   [SHA256_HASH '...'] [ABI ...] [DETERMINISTIC] [SETTINGS ...]
--
-- Every statement below is handed to formatQuery(), which parses it with
-- ParserQuery and prints the resulting AST. A statement the parser accepts
-- therefore round-trips to its canonical text; a statement it rejects raises
-- SYNTAX_ERROR. No function is ever created, so no driver is invoked, no WASM
-- module is loaded and no global state is touched.

-- The function name must be an identifier, not a literal.
SELECT formatQuery($$CREATE FUNCTION 42 AS x -> x$$); -- { serverError SYNTAX_ERROR }
-- ON must introduce a CLUSTER clause.
SELECT formatQuery($$CREATE FUNCTION f ON 42 AS x -> x$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ON CLUSTER c AS x -> x$$);

-- Driver form, ARGUMENTS clause: a parenthesised list is required, and it holds
-- either name/type pairs, or bare types, or nothing at all.
SELECT formatQuery($$CREATE FUNCTION f ARGUMENTS x ENGINE = n() AS 'c'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ARGUMENTS (a Int32 ENGINE = n() AS 'c'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ARGUMENTS (+) ENGINE = n() AS 'c'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ARGUMENTS () ENGINE = n() AS 'c'$$);
SELECT formatQuery($$CREATE FUNCTION f ARGUMENTS (Int32, String) ENGINE = n() AS 'c'$$);

-- RETURNS takes a data type.
SELECT formatQuery($$CREATE FUNCTION f RETURNS 42 ENGINE = n() AS 'c'$$); -- { serverError SYNTAX_ERROR }

-- ENGINE takes a driver name followed by optional `key = literal` arguments.
SELECT formatQuery($$CREATE FUNCTION f ENGINE = 42 AS 'c'$$); -- { serverError SYNTAX_ERROR }
-- A key that is not an identifier, a key without `=`, a value that is not a
-- literal, and an argument list that is never closed.
SELECT formatQuery($$CREATE FUNCTION f ENGINE = n(1 = 2) AS 'c'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ENGINE = n(opt_level '2') AS 'c'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ENGINE = n(opt_level = +) AS 'c'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ENGINE = n(opt_level = '2' AS 'c'$$); -- { serverError SYNTAX_ERROR }
-- The code body is mandatory for the driver form, and it must be a string.
SELECT formatQuery($$CREATE FUNCTION f ENGINE = n()$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f ENGINE = n() AS 42$$); -- { serverError SYNTAX_ERROR }
-- Two engine arguments: the comma separator and both key/value pairs survive
-- the round trip.
SELECT formatQuery($$CREATE FUNCTION f ENGINE = n(opt_level = '2', std = 'c11') AS 'return 1'$$);

-- ARGUMENTS/RETURNS belong to the driver form, so they cannot precede a lambda.
SELECT formatQuery($$CREATE FUNCTION f ARGUMENTS () AS x -> x$$); -- { serverError SYNTAX_ERROR }
-- ATTACH FUNCTION is only meaningful for the driver form.
SELECT formatQuery($$ATTACH FUNCTION f AS x -> x$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$ATTACH FUNCTION f ARGUMENTS (a Int32) RETURNS UInt8 ENGINE = n(opt_level = '0') AS 'return a'$$);

-- Lambda form: AS must be followed by an expression.
SELECT formatQuery($$CREATE FUNCTION f AS +$$); -- { serverError SYNTAX_ERROR }

-- LANGUAGE takes an identifier, and WASM is the only supported language.
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE 42$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE JS$$); -- { serverError SYNTAX_ERROR }

-- WASM form, ARGUMENTS clause: same list shapes as the driver form.
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE WASM ARGUMENTS (a Int32 RETURNS UInt8 FROM 'm'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE WASM ARGUMENTS (+) RETURNS UInt8 FROM 'm'$$); -- { serverError SYNTAX_ERROR }
-- The symbol after `::` and the SHA256_HASH value are string literals.
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE WASM ARGUMENTS (a Int32) RETURNS UInt8 FROM 'm' :: 42$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE WASM ARGUMENTS (a Int32) RETURNS UInt8 FROM 'm' SHA256_HASH 42$$); -- { serverError SYNTAX_ERROR }
-- ARGUMENTS and FROM are both mandatory.
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE WASM RETURNS UInt8 FROM 'm'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE WASM ARGUMENTS (a Int32) RETURNS UInt8$$); -- { serverError SYNTAX_ERROR }
-- SETTINGS takes `name = value` pairs.
SELECT formatQuery($$CREATE FUNCTION f LANGUAGE WASM ARGUMENTS (a Int32) RETURNS UInt8 FROM 'm' SETTINGS 1$$); -- { serverError SYNTAX_ERROR }
-- All WASM clauses at once. They may be written in any order; the printed text
-- shows the canonical order that formatImpl uses, so SHA256_HASH moves after
-- ABI and DETERMINISTIC.
SELECT formatQuery($$CREATE FUNCTION w LANGUAGE WASM SHA256_HASH 'cafe42' ARGUMENTS (a Int32, b UInt8) RETURNS UInt64 FROM 'mod' :: 'fn' ABI ROW_DIRECT DETERMINISTIC SETTINGS serialization_format = 'CSV'$$);
