SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

SELECT
    loyalty,
    count()
FROM test.hits ANY LEFT JOIN
(
    SELECT
        UserID,
        sum(SearchEngineID = 2) AS yandex,
        sum(SearchEngineID = 3) AS google,
        toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    FROM test.hits
    WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
    GROUP BY UserID
    HAVING (yandex + google) > 10
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT
    loyalty,
    count()
FROM
(
    SELECT UserID
    FROM test.hits
) ANY LEFT JOIN
(
    SELECT
        UserID,
        sum(SearchEngineID = 2) AS yandex,
        sum(SearchEngineID = 3) AS google,
        toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    FROM test.hits
    WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
    GROUP BY UserID
    HAVING (yandex + google) > 10
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT
    loyalty,
    count()
FROM
(
    SELECT
        loyalty,
        UserID
    FROM
    (
        SELECT UserID
        FROM test.hits
    ) ANY LEFT JOIN
    (
        SELECT
            UserID,
            sum(SearchEngineID = 2) AS yandex,
            sum(SearchEngineID = 3) AS google,
            toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
        FROM test.hits
        WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
        GROUP BY UserID
        HAVING (yandex + google) > 10
    ) USING UserID
)
GROUP BY loyalty
ORDER BY loyalty ASC;


SELECT
    loyalty,
    count() AS c,
    bar(log(c + 1) * 1000, 0, log(3000000) * 1000, 80)
FROM test.hits ANY INNER JOIN
(
    SELECT
        UserID,
        toInt8(if(yandex > google, yandex / (yandex + google), -google / (yandex + google)) * 10) AS loyalty
    FROM
    (
        SELECT
            UserID,
            sum(SearchEngineID = 2) AS yandex,
            sum(SearchEngineID = 3) AS google
        FROM test.hits
        WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
        GROUP BY UserID
        HAVING (yandex + google) > 10
    )
) USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;
