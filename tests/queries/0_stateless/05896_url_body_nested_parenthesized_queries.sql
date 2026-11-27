-- A parenthesized query in `body(...)` is parsed once: nested `body((SELECT ...))` used to parse the inner query
-- twice at every level, so the time of parsing was exponential in the depth of nesting.

SELECT formatQuerySingleLine('SELECT body((SELECT body((SELECT body((SELECT 1))))))');
SELECT formatQuerySingleLine('SELECT body((SELECT body((SELECT body((SELECT 1), \'CSV\')), \'CSV\')), \'CSV\')');
SELECT formatQuerySingleLine('SELECT body((SELECT body((SELECT body((SELECT 1 FORMAT CSV)) FORMAT CSV)) FORMAT CSV))');

SELECT length(formatQuerySingleLine('SELECT ' || repeat('body((SELECT ', 40) || '1' || repeat('))', 40)));
SELECT length(formatQuerySingleLine('SELECT ' || repeat('body((SELECT ', 40) || '1' || repeat('), \'CSV\')', 40)));
SELECT length(formatQuerySingleLine('SELECT ' || repeat('body((SELECT ', 40) || '1' || repeat(' FORMAT CSV))', 40)));

-- A parenthesized query is a usual operand of an expression.
SELECT formatQuerySingleLine('SELECT body((SELECT 1) + 1, 2), body((SELECT 1) AS x), body(((SELECT 1), 2)), body((1 + 2) * 3)');

-- The backtracks of an unsuccessful attempt to parse a query are counted.
SELECT formatQuerySingleLine('SELECT ' || repeat('body((SELECT ', 40) || '1' || repeat(' ]', 40)) SETTINGS max_parser_backtracks = 10000; -- { serverError TOO_SLOW_PARSING }
