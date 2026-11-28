-- A LATERAL subquery is evaluated once per distinct value of the correlated columns, so `filesystemCache`
-- with the 'random' order, which draws a new sample on every read, is rejected like `generateRandom`.
-- The 'hottest' and 'coldest' orders are allowed.

SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM filesystemCache('local_cache', 10, 1000000000, 'random') WHERE size > o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

-- The number of segments depends on other tests which use `local_cache`, so only check that the query runs.
SELECT o.id, l.c >= 0 FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM filesystemCache('local_cache', 10, 1000000000, 'hottest') WHERE size > o.k) AS l ON true
ORDER BY o.id;

SELECT o.id, l.c >= 0 FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM filesystemCache('local_cache', 10, 1000000000, 'coldest') WHERE size > o.k) AS l ON true
ORDER BY o.id;

DROP TABLE outer_t;
