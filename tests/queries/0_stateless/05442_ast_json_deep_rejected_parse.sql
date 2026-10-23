-- A deeply nested JSON document that fails to parse must be rejected without overflowing the stack
-- while the document built so far is released.

-- The whole document is built before the trailing characters are rejected.
SELECT formatQueryFromJSON(repeat('[', 40000) || repeat(']', 40000) || 'x') SETTINGS max_ast_depth = 100000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError BAD_ARGUMENTS, TOO_DEEP_RECURSION }
SELECT formatQueryFromJSON('{"a":' || repeat('[', 40000) || repeat(']', 40000) || '}x') SETTINGS max_ast_depth = 100000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError BAD_ARGUMENTS, TOO_DEEP_RECURSION }

-- An unterminated document is rejected after all of its levels are built.
SELECT formatQueryFromJSON(repeat('[', 40000)) SETTINGS max_ast_depth = 100000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError BAD_ARGUMENTS, TOO_DEEP_RECURSION }
SELECT formatQueryFromJSON('{"a":' || repeat('[', 40000)) SETTINGS max_ast_depth = 100000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError BAD_ARGUMENTS, TOO_DEEP_RECURSION }

SELECT 'ok';
