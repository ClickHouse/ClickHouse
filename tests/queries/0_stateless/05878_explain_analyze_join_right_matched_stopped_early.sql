-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- A RIGHT or FULL hash join derives its matched right rows from the right rows it emits without a
-- partner after all probing, so a query that stops before that reports them as `not collected`.
-- 100 of the 1000 right keys (multiples of 10 below 1000) have a partner among the 1000 left keys.

SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS empty_right;
CREATE TABLE empty_right (k UInt64) ENGINE = Memory;

SELECT 'RIGHT', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers(1000)) AS l RIGHT JOIN (SELECT number * 10 AS k FROM numbers(1000)) AS r ON l.k = r.k)
WHERE explain LIKE '%Right: rows%';

SELECT 'RIGHT LIMIT 1', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers(1000)) AS l RIGHT JOIN (SELECT number * 10 AS k FROM numbers(1000)) AS r ON l.k = r.k LIMIT 1)
WHERE explain LIKE '%Right: rows%';

SELECT 'FULL', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers(1000)) AS l FULL JOIN (SELECT number * 10 AS k FROM numbers(1000)) AS r ON l.k = r.k)
WHERE explain LIKE '%Right: rows%';

SELECT 'FULL LIMIT 1', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers(1000)) AS l FULL JOIN (SELECT number * 10 AS k FROM numbers(1000)) AS r ON l.k = r.k LIMIT 1)
WHERE explain LIKE '%Right: rows%';

SELECT 'RIGHT parallel_hash', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers_mt(1000)) AS l RIGHT JOIN (SELECT number * 10 AS k FROM numbers(1000)) AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 4)
WHERE explain LIKE '%Right: rows%';

-- Nothing matches, so a `LIMIT` stops the join while it emits the unmatched right rows, 10 per block.
SELECT 'RIGHT unmatched', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers(10)) AS l RIGHT JOIN (SELECT number + 1000 AS k FROM numbers(1000)) AS r ON l.k = r.k SETTINGS max_block_size = 10)
WHERE explain LIKE '%Right: rows%';

SELECT 'RIGHT unmatched LIMIT 1', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers(10)) AS l RIGHT JOIN (SELECT number + 1000 AS k FROM numbers(1000)) AS r ON l.k = r.k LIMIT 1 SETTINGS max_block_size = 10)
WHERE explain LIKE '%Right: rows%';

-- An empty right side has no row to match.
SELECT 'FULL empty right', extract(explain, 'matched (not collected|[0-9]+)')
FROM (EXPLAIN ANALYZE SELECT l.k, r.k FROM (SELECT number AS k FROM numbers(10)) AS l FULL JOIN empty_right AS r ON l.k = r.k)
WHERE explain LIKE '%Right: rows%';

DROP TABLE empty_right;
