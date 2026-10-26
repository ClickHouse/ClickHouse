-- The right side arrives in three blocks: a new key, a block whose only key is already in the map, and another new
-- key. `ANY` inserts nothing from the second block, so the join drops it and must keep the third.

SET join_algorithm = 'parallel_hash';
SET max_threads = 1;
SET max_block_size = 1;
SET min_joined_block_size_rows = 0;
SET min_joined_block_size_bytes = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

SELECT 'two-level';
SELECT l.k, r.v
FROM (SELECT number + 1 AS k FROM numbers(2)) AS l
ANY LEFT JOIN (SELECT if(number < 2, 1, 2) AS k, number AS v FROM numbers(3)) AS r ON l.k = r.k
ORDER BY l.k
SETTINGS parallel_hash_join_threshold = 0;

SELECT 'serial';
SELECT l.k, r.v
FROM (SELECT number + 1 AS k FROM numbers(2)) AS l
ANY LEFT JOIN (SELECT if(number < 2, 1, 2) AS k, number AS v FROM numbers(3)) AS r ON l.k = r.k
ORDER BY l.k
SETTINGS parallel_hash_join_threshold = 18446744073709551615;
