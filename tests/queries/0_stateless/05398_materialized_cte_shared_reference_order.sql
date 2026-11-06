SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

WITH
    prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x),
    prometheus_query_step_2 AS MATERIALIZED
    (
        SELECT l.x FROM prometheus_query_step_1 AS l
        INNER JOIN prometheus_query_step_1 AS r ON l.x = r.x
    ),
    prometheus_query_step_3 AS MATERIALIZED
    (
        SELECT l.x FROM prometheus_query_step_2 AS l
        INNER JOIN prometheus_query_step_2 AS r ON l.x = r.x
    )
SELECT count()
FROM prometheus_query_step_3 AS p
CROSS JOIN prometheus_query_step_3 AS q
CROSS JOIN prometheus_query_step_2 AS s;

WITH
    prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x),
    prometheus_query_step_2 AS MATERIALIZED
    (
        SELECT l.x FROM prometheus_query_step_1 AS l
        INNER JOIN prometheus_query_step_1 AS r ON l.x = r.x
    ),
    prometheus_query_step_3 AS MATERIALIZED
    (
        SELECT l.x FROM prometheus_query_step_2 AS l
        INNER JOIN prometheus_query_step_2 AS r ON l.x = r.x
    )
SELECT count()
FROM prometheus_query_step_2 AS s
CROSS JOIN prometheus_query_step_3 AS p
CROSS JOIN prometheus_query_step_3 AS q;

WITH
    prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x),
    prometheus_query_step_2 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_1 a CROSS JOIN prometheus_query_step_1 b),
    prometheus_query_step_3 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_2 a CROSS JOIN prometheus_query_step_2 b),
    prometheus_query_step_4 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_3 a CROSS JOIN prometheus_query_step_3 b CROSS JOIN prometheus_query_step_2 c),
    prometheus_query_step_5 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_4 a CROSS JOIN prometheus_query_step_4 b)
SELECT count()
FROM prometheus_query_step_5 d
CROSS JOIN prometheus_query_step_5 e
CROSS JOIN prometheus_query_step_4 f;

WITH
    prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x),
    prometheus_query_step_2 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_1 a CROSS JOIN prometheus_query_step_1 b),
    prometheus_query_step_3 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_2 a CROSS JOIN prometheus_query_step_2 b),
    prometheus_query_step_4 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_3 a CROSS JOIN prometheus_query_step_3 b CROSS JOIN prometheus_query_step_2 c),
    prometheus_query_step_5 AS MATERIALIZED (SELECT a.x FROM prometheus_query_step_4 a CROSS JOIN prometheus_query_step_4 b)
SELECT count()
FROM prometheus_query_step_4 f
CROSS JOIN prometheus_query_step_5 d
CROSS JOIN prometheus_query_step_5 e;

WITH prometheus_query_step_1 AS MATERIALIZED
(
    SELECT number AS x, number + 10 AS y FROM numbers(3)
)
SELECT * FROM prometheus_query_step_1
WHERE y = (SELECT max(y) FROM prometheus_query_step_1);

WITH prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x, 2 AS y)
SELECT x FROM prometheus_query_step_1;
