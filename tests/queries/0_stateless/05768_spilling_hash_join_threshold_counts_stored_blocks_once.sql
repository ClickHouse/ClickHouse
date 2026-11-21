-- A hash join counts its stored right-side blocks once and its hash table twice towards max_bytes_before_external_join.
-- Random settings limits: max_threads=(1, 2)

SET join_algorithm = 'hash', max_bytes_ratio_before_external_join = 0, collect_hash_table_stats_during_joins = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0;

-- ~40 MB of stored right-side blocks in 20 blocks, a small hash table.
SELECT count(), sum(length(r.s))
FROM (SELECT number AS k FROM numbers(20000)) AS l
INNER JOIN (SELECT number AS k, repeat(toString(number % 10), 2000) AS s FROM numbers(20000) SETTINGS max_block_size = 1000) AS r USING (k)
SETTINGS max_bytes_before_external_join = 57000000, hash_join_layout = 'serial', log_comment = 'fits_serial';

SELECT count(), sum(length(r.s))
FROM (SELECT number AS k FROM numbers(20000)) AS l
INNER JOIN (SELECT number AS k, repeat(toString(number % 10), 2000) AS s FROM numbers(20000) SETTINGS max_block_size = 1000) AS r USING (k)
SETTINGS max_bytes_before_external_join = 57000000, hash_join_layout = 'parallel', log_comment = 'fits_parallel';

SELECT count(), sum(length(r.s))
FROM (SELECT number AS k FROM numbers(20000)) AS l
INNER JOIN (SELECT number AS k, repeat(toString(number % 10), 2000) AS s FROM numbers(20000) SETTINGS max_block_size = 1000) AS r USING (k)
SETTINGS max_bytes_before_external_join = 10000000, hash_join_layout = 'parallel', log_comment = 'spills';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('fits_serial', 'fits_parallel', 'spills')
ORDER BY log_comment;
