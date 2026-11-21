-- Random settings limits: query_plan_optimize_join_order_limit=(10, None); use_statistics=(1, 1); materialize_statistics_on_insert=(1, 1); query_plan_optimize_join_order_randomize=(0, 0)
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dim;
CREATE TABLE src (k UInt32, g UInt32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic';
CREATE TABLE dim (g UInt32) ENGINE = MergeTree ORDER BY g SETTINGS auto_statistics_types = 'basic';
INSERT INTO src VALUES (1, 7);
INSERT INTO dim VALUES (7);

SELECT '-- a GROUP BY over a filtered read of one row';
SELECT extract(explain, 'q\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM (SELECT g FROM src WHERE k = 1 GROUP BY g) AS q INNER JOIN dim AS d USING (g))
WHERE explain LIKE '%⋈%' LIMIT 1;
SELECT count() FROM (SELECT g FROM src WHERE k = 1 GROUP BY g) AS q INNER JOIN dim AS d USING (g);

SELECT '-- a GROUP BY over an unfiltered read of one row';
SELECT extract(explain, 'q\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM (SELECT g FROM src GROUP BY g) AS q INNER JOIN dim AS d USING (g))
WHERE explain LIKE '%⋈%' LIMIT 1;

DROP TABLE src;
DROP TABLE dim;
