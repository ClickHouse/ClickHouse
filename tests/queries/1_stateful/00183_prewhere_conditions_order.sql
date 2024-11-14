SET optimize_move_to_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;

SELECT explain
FROM (
EXPLAIN actions=1
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
FROM test.hits
WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10
SETTINGS allow_reorder_prewhere_conditions = 0
)
WHERE explain like '%Prewhere filter column%';

SELECT explain
FROM (
EXPLAIN actions=1
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
FROM test.hits
WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC
LIMIT 10
SETTINGS allow_reorder_prewhere_conditions = 1
)
WHERE explain like '%Prewhere filter column%';
