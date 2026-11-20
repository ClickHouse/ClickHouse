-- Random settings limits: execute_exists_as_scalar_subquery=(1, 1)
-- The empty grouping set `()` returns a row even for an empty input, so `EXISTS` over it is true.

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (k UInt64) ENGINE = Memory;
INSERT INTO tab VALUES (1);

SELECT 'empty input', EXISTS (SELECT k FROM tab WHERE k = 5 GROUP BY GROUPING SETS ((k), ()));
SELECT 'empty input, aggregate', EXISTS (SELECT k, count() FROM tab WHERE k = 5 GROUP BY GROUPING SETS ((k), ()));
SELECT 'empty input, LIMIT', EXISTS (SELECT k FROM tab WHERE k = 5 GROUP BY GROUPING SETS ((k), ()) LIMIT 1);
SELECT 'empty input, NOT EXISTS', NOT EXISTS (SELECT k FROM tab WHERE k = 5 GROUP BY GROUPING SETS ((k), ()));
SELECT 'empty input, keyed sets', EXISTS (SELECT k FROM tab WHERE k = 5 GROUP BY GROUPING SETS ((k), (k, k + 1)));
SELECT 'empty input, suppressed', EXISTS (SELECT k FROM tab WHERE k = 5 GROUP BY GROUPING SETS ((k), ())) SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT 'empty input, arrayJoin above', EXISTS (SELECT arrayJoin(CAST([], 'Array(UInt8)')) FROM tab WHERE k = 5 GROUP BY GROUPING SETS ((k), ()));

SELECT 'correlated', count() FROM numbers(3) AS l WHERE EXISTS (SELECT tab.k FROM tab WHERE tab.k = l.number GROUP BY GROUPING SETS ((tab.k), ()));
SELECT 'correlated, NOT EXISTS', count() FROM numbers(3) AS l WHERE NOT EXISTS (SELECT tab.k FROM tab WHERE tab.k = l.number GROUP BY GROUPING SETS ((tab.k), ()));
SELECT 'correlated, projection', l.number, EXISTS (SELECT tab.k FROM tab WHERE tab.k = l.number GROUP BY GROUPING SETS ((tab.k), ())) FROM numbers(3) AS l ORDER BY l.number;
SELECT 'correlated, keyed sets', count() FROM numbers(3) AS l WHERE EXISTS (SELECT tab.k FROM tab WHERE tab.k = l.number GROUP BY GROUPING SETS ((tab.k), (tab.k, tab.k + 1)));
SELECT 'correlated, suppressed', count() FROM numbers(3) AS l WHERE EXISTS (SELECT tab.k FROM tab WHERE tab.k = l.number GROUP BY GROUPING SETS ((tab.k), ())) SETTINGS empty_result_for_aggregation_by_empty_set = 1;

DROP TABLE tab;
