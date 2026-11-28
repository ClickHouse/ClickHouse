-- Every pipe operator accepts exactly the clause body documented in the "PIPE OPERATORS" block of
-- ParserPipeOperators::getDocumentation(), and a malformed body is a syntax error instead of being
-- silently ignored. The query text is given to formatQuery(), so the parser runs on the server and
-- the error code comes back from the server; a pipe query written directly in a test is rejected by
-- the client (see 04613_pipe_operators.sql).

SELECT '-- A select-list operator needs a non-empty expression list';
SELECT formatQuery($$FROM numbers(1) |> SELECT$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> EXTEND$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> AGGREGATE$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> AGGREGATE count() GROUP BY$$); -- { serverError SYNTAX_ERROR }

SELECT '-- SET takes identifier = expression replacements';
SELECT formatQuery($$FROM numbers(1) |> SET 1 = 2$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> SET a b$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> SET a =$$); -- { serverError SYNTAX_ERROR }
-- The second replacement is malformed, so its comma is left for an outer list and ends the query
SELECT formatQuery($$FROM numbers(1) |> SET number = 1, 2$$); -- { serverError SYNTAX_ERROR }

SELECT '-- DROP takes a list of column names';
SELECT formatQuery($$FROM numbers(1) |> DROP 1$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> DROP number, 1$$); -- { serverError SYNTAX_ERROR }

SELECT '-- AS takes a single identifier';
SELECT formatQuery($$FROM numbers(1) |> AS 1$$); -- { serverError SYNTAX_ERROR }

SELECT '-- ORDER BY, LIMIT and OFFSET need their expressions';
SELECT formatQuery($$FROM numbers(1) |> ORDER BY$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> LIMIT$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> LIMIT 1 OFFSET$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> OFFSET$$); -- { serverError SYNTAX_ERROR }

SELECT '-- A trailing SETTINGS clause of a generated wrapper needs name = value pairs';
SELECT formatQuery($$FROM numbers(1) |> LIMIT 1 SETTINGS 1$$); -- { serverError SYNTAX_ERROR }

SELECT '-- A set operation operand must be a query, and parenthesized before another pipe operator';
SELECT formatQuery($$FROM numbers(1) |> UNION ALL 5$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> UNION ALL (FROM numbers(1)), 5$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuery($$FROM numbers(1) |> UNION ALL SELECT 1 |> WHERE 1$$); -- { serverError SYNTAX_ERROR }

SELECT '-- A well-formed operator produces the same query as the hand-written nested form';
SELECT formatQuery($$FROM numbers(1) |> AS x$$) = formatQuery($$SELECT * FROM (SELECT * FROM numbers(1)) AS x$$);
SELECT formatQuery($$FROM numbers(1) |> UNION ALL (FROM numbers(1)), (FROM numbers(2))$$) = formatQuery($$SELECT * FROM numbers(1) UNION ALL SELECT * FROM numbers(1) UNION ALL SELECT * FROM numbers(2)$$);
SELECT formatQuery($$FROM numbers(1) |> AS x |> UNION ALL (FROM numbers(2))$$) = formatQuery($$SELECT * FROM (SELECT * FROM numbers(1)) AS x UNION ALL SELECT * FROM numbers(2)$$);

SELECT '-- and it returns the rows of that nested form';
SELECT count() FROM (FROM numbers(3) |> AS x);
SELECT count() FROM (FROM numbers(1) |> UNION ALL (FROM numbers(1)), (FROM numbers(2)));
SELECT count() FROM (FROM numbers(1) |> AS x |> UNION ALL (FROM numbers(2)));
