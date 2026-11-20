-- A `JOIN` converted to `IN` puts the plan of its set back into the plan it was extracted from.
-- The set plan must not copy the resources of the whole plan, or every conversion would add another copy.

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO l SELECT number FROM numbers(10);
INSERT INTO r SELECT number FROM numbers(10);

SET query_plan_convert_join_to_in = 1;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;
SET query_plan_max_optimizations_to_apply = 0;
SET max_memory_usage = 134217728;

WITH
    v0 AS (SELECT l.k AS k FROM l INNER JOIN r ON l.k = r.k),
    v1 AS (SELECT k FROM v0 UNION ALL SELECT k FROM v0),
    v2 AS (SELECT k FROM v1 UNION ALL SELECT k FROM v1),
    v3 AS (SELECT k FROM v2 UNION ALL SELECT k FROM v2),
    v4 AS (SELECT k FROM v3 UNION ALL SELECT k FROM v3),
    v5 AS (SELECT k FROM v4 UNION ALL SELECT k FROM v4),
    v6 AS (SELECT k FROM v5 UNION ALL SELECT k FROM v5),
    v7 AS (SELECT k FROM v6 UNION ALL SELECT k FROM v6),
    v8 AS (SELECT k FROM v7 UNION ALL SELECT k FROM v7),
    v9 AS (SELECT k FROM v8 UNION ALL SELECT k FROM v8)
SELECT count() FROM v9;

DROP TABLE l;
DROP TABLE r;
