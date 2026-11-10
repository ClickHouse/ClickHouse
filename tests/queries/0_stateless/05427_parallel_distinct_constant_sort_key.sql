-- A full sort by constant keys over the parallel final DISTINCT's streams must not wait on one stream (Pipeline stuck).

SET allow_parallel_distinct = 1, max_threads = 4, max_block_size = 100, query_plan_enable_optimizations = 1;

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT 'x' AS c, number FROM numbers_mt(100000)) ORDER BY c) WHERE explain ILIKE '%ScatterByPartition%';

SELECT count() FROM (SELECT c FROM (SELECT DISTINCT 'x' AS c, number FROM numbers_mt(100000)) ORDER BY c LIMIT 50000);

SELECT max(rn) FROM (SELECT row_number() OVER (ORDER BY 'x') AS rn FROM (SELECT DISTINCT number FROM numbers_mt(100000)));
