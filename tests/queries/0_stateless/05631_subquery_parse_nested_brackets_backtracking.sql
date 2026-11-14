-- A failed reading of a subquery was retried at the same bracket by every enclosing alternative
-- (a parenthesized expression, a function argument, a quantified comparison, a union element),
-- so the parse time was exponential in the nesting of such brackets: this query exceeded
-- the limit of backtracks instead of failing with a syntax error. Found by `select_parser_fuzzer`.

SELECT formatQuery('select 00 = all (select (((+(((+(((((((f(((SELECT (((((((J((((( (select (((+(((+(((((((f(((SELECT (((((((J((((( (select (((+(((+(((((((f(((SELECT (((((((J((((( (select (((+(((+(((((((f(((SELECT (((((((J((((( 1')
SETTINGS max_parser_depth = 300, max_parser_backtracks = 1000000; -- { serverError SYNTAX_ERROR }

-- The same query with closed brackets parses.
SELECT length(formatQuery('select 00 = all (select (((+(((+(((((((f(((SELECT (((((((J((((( (select (((+(((+(((((((f(((SELECT (((((((J((((( 1))))))))))))))))))))))))))))))))))))))))))))))))))))))))))')) > 0
SETTINGS max_parser_depth = 300, max_parser_backtracks = 1000000;
