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
