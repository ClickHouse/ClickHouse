SELECT x, y FROM (SELECT 5 AS x, 'Hello' AS y) ORDER BY x WITH FILL FROM 3 TO 7, y, x WITH FILL FROM 1 TO 10; -- { serverError 475 }
