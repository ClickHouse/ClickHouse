-- CASE and INTERVAL used as identifiers or function names inside deep nesting.

SELECT (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + (interval + 1)))))))))))))))))))))))))))))) FROM (SELECT 1 AS `interval`);
SELECT (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + (case + 1)))))))))))))))))))))))))))))) FROM (SELECT 1 AS `case`);

SELECT length(formatQuery('SELECT ' || repeat('case(', 30) || '1' || repeat(')', 30))) > 0;
SELECT length(formatQuery('SELECT ' || repeat('interval(', 30) || '1' || repeat(')', 30))) > 0;

SELECT formatQuery('SELECT ' || repeat('CASE(', 30)); -- { serverError SYNTAX_ERROR }
SELECT defaultValueOfTypeName('SimpleAggregateFunction(any(' || repeat('CASE(', 30)); -- { serverError SYNTAX_ERROR }

SELECT formatQuerySingleLine('SELECT case(CASE WHEN 1 THEN 2 END), CASE (case(1)) WHEN 1 THEN 2 END, interval(INTERVAL 1 DAY), INTERVAL (interval(1)) DAY, INTERVAL 1 DAY + interval');
