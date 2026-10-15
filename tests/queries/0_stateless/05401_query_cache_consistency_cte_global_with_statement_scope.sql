-- With `query_cache_use_only_when_data_was_not_changed`, the tables a query reads are found after
-- substituting its CTEs. A subquery with `enable_global_with_statement = 0` does not see the CTEs of
-- the enclosing query, so there a CTE name refers to a real table of that name, and that table must be
-- tracked: an insert into it must not be answered from the cache.

-- The cache key includes the current database, so this test does not need to clear the server-wide
-- query cache.

DROP TABLE IF EXISTS q;
CREATE TABLE q (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO q VALUES (1);

SET use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- The inner query reads the table `q`, not the CTE.
WITH q AS (SELECT 999 AS x) SELECT sum(x) FROM (SELECT x FROM q SETTINGS enable_global_with_statement = 0);
INSERT INTO q VALUES (2);
WITH q AS (SELECT 999 AS x) SELECT sum(x) FROM (SELECT x FROM q SETTINGS enable_global_with_statement = 0);

-- Where the subquery does see the CTE, it reads the CTE.
WITH q AS (SELECT 999 AS x) SELECT sum(x) FROM (SELECT x FROM q);

DROP TABLE q;
