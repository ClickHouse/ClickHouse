-- A deeply nested JSON document that parses but is not an object at the root must be rejected
-- without overflowing the stack while the parsed document is released.
SELECT formatQueryFromJSON(repeat('[', 40000) || repeat(']', 40000)) SETTINGS max_ast_depth = 100000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError BAD_ARGUMENTS, TOO_DEEP_RECURSION }
SELECT formatQueryFromJSON('{"a":' || repeat('[', 40000) || repeat(']', 40000) || '}') SETTINGS max_ast_depth = 100000, max_ast_elements = 0, max_query_size = 100000000; -- { serverError BAD_ARGUMENTS, TOO_DEEP_RECURSION }
SELECT 'ok';
