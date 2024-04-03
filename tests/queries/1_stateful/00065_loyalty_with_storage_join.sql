USE test;

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

DROP TABLE IF EXISTS join;
CREATE TABLE join (UserID UInt64, loyalty Int8) ENGINE = Join(SEMI, LEFT, UserID);

INSERT INTO join
SELECT
    UserID,
    toInt8(if((sum(SearchEngineID = 2) AS yandex) > (sum(SearchEngineID = 3) AS google),
    yandex / (yandex + google),
    -google / (yandex + google)) * 10) AS loyalty
FROM hits
WHERE (SearchEngineID = 2) OR (SearchEngineID = 3)
GROUP BY UserID
HAVING (yandex + google) > 10;

SELECT
    loyalty,
    count()
FROM hits SEMI LEFT JOIN join USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;

DETACH TABLE join;
ATTACH TABLE join;

SELECT
    loyalty,
    count()
FROM hits SEMI LEFT JOIN join USING UserID
GROUP BY loyalty
ORDER BY loyalty ASC;

DROP TABLE join;
