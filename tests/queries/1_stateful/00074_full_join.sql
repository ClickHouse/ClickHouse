set any_join_distinct_right_table_keys = 1;
set joined_subquery_requires_alias = 0;

SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        (CounterID % 100000) AS CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY FULL OUTER JOIN
(
    SELECT
        (CounterID % 100000) AS CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
    HAVING visits > 0
) USING CounterID
WHERE hits = 0 OR visits = 0
ORDER BY
    hits + visits * 10 DESC,
    CounterID ASC
LIMIT 20;


SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        (CounterID % 100000) AS CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        (CounterID % 100000) AS CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
    HAVING visits > 0
) USING CounterID
WHERE hits = 0 OR visits = 0
ORDER BY
    hits + visits * 10 DESC,
    CounterID ASC
LIMIT 20;


SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        (CounterID % 100000) AS CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY RIGHT JOIN
(
    SELECT
        (CounterID % 100000) AS CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
    HAVING visits > 0
) USING CounterID
WHERE hits = 0 OR visits = 0
ORDER BY
    hits + visits * 10 DESC,
    CounterID ASC
LIMIT 20;


SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        (CounterID % 100000) AS CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY INNER JOIN
(
    SELECT
        (CounterID % 100000) AS CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
    HAVING visits > 0
) USING CounterID
WHERE hits = 0 OR visits = 0
ORDER BY
    hits + visits * 10 DESC,
    CounterID ASC
LIMIT 20;
