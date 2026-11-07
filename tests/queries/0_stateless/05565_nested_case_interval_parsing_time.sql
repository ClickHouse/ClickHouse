-- `CASE` and `INTERVAL` are tried as keywords and then as identifiers. Nested ones that cannot be completed as
-- keywords used to retry every combination of the two readings, which takes time exponential in the depth.
-- Now they fail with a syntax error well within a small backtracking budget.
SET max_parser_backtracks = 10000;

SELECT formatQuery('SELECT ' || repeat('CASE(', 40) || '1'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT ' || repeat('f(CASE(', 40) || '1'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT ' || repeat('CASE+(', 40) || '1'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT ' || repeat('INTERVAL(', 40) || '1'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT ' || repeat('INTERVAL+(', 40) || '1'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT ' || repeat('CASE ', 1000) || '1'); -- { serverError SYNTAX_ERROR }
SELECT defaultValueOfTypeName('SimpleAggregateFunction(any(' || repeat('CASE(', 40) || '1'); -- { serverError SYNTAX_ERROR }

-- The keyword forms.
SELECT CASE WHEN 1 THEN 'a' END, CASE 1 WHEN 1 THEN 'b' ELSE 'c' END, CASE (1) WHEN 1 THEN 'd' END;
SELECT CASE WHEN 1 THEN CASE WHEN 1 THEN 'e' END END, [CASE WHEN 1 THEN 'f' END], (CASE WHEN 1 THEN 'g' END);
SELECT INTERVAL 1 DAY, INTERVAL (1 + 2) HOUR, INTERVAL '1 day', INTERVAL '1:30' HOUR TO MINUTE, [INTERVAL 1 SECOND];
SELECT formatQuery('SELECT CASE WHEN x THEN CASE (y) WHEN 1 THEN INTERVAL (z + 1) MINUTE END END');

-- The identifier forms, also next to the keyword forms.
SELECT case, interval FROM (SELECT 1 AS case, 2 AS interval);
SELECT case + case, interval + (interval + 1) FROM (SELECT 1 AS case, 2 AS interval);
SELECT INTERVAL interval DAY FROM (SELECT 2 AS interval);
