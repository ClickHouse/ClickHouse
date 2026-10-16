-- Regression for ClickHouse/ClickHouse#61195: FETCH ... ONLY does not need ORDER BY.
SELECT 1 FETCH NEXT 1 ROW ONLY;
SELECT count() FROM (SELECT number FROM numbers(10) FETCH FIRST 3 ROWS ONLY);
SELECT count() FROM (SELECT number FROM numbers(10) FETCH NEXT 2 ROWS ONLY);
SELECT count() FROM (SELECT number FROM numbers(10) OFFSET 4 ROWS FETCH NEXT 3 ROWS ONLY);
SELECT count() FROM (SELECT number FROM numbers(10) FETCH FIRST 0 ROWS ONLY);
SELECT count() FROM (SELECT number FROM numbers(10) FETCH FIRST 20 ROWS ONLY);

-- FETCH ... ONLY and LIMIT normalize to the same SQL.
SELECT formatQuerySingleLine('SELECT number FROM numbers(10) FETCH FIRST 3 ROWS ONLY')
    = formatQuerySingleLine('SELECT number FROM numbers(10) LIMIT 3');
SELECT formatQuerySingleLine('SELECT number FROM numbers(10) OFFSET 4 ROWS FETCH NEXT 3 ROWS ONLY')
    = formatQuerySingleLine('SELECT number FROM numbers(10) LIMIT 3 OFFSET 4');

-- WITH TIES still requires ORDER BY; test it through the parser without executing it.
SELECT isNull(formatQueryOrNull('SELECT number FROM numbers(10) FETCH FIRST 2 ROWS WITH TIES'));
SELECT isNull(formatQueryOrNull('SELECT number FROM numbers(10) OFFSET 4 ROWS FETCH NEXT 2 ROWS WITH TIES'));

-- FETCH WITH TIES with ORDER BY still works.
SELECT count() FROM
(
    SELECT number % 2 AS x
    FROM numbers(4)
    ORDER BY x
    FETCH FIRST 1 ROW WITH TIES
);
