-- Tags: long, no-tsan
-- no-tsan: millions of keys, too slow under TSan
-- Random settings limits: query_plan_optimize_join_order_limit=(1, None); max_block_size=(65409, 65409)
-- With the limit at 0 the join collects no hash table statistics, so the second run would be cold too. The block size is
-- fixed because the join counts the allocated bytes of the right blocks, and those depend on it.

-- With `enable_robin_hood_hash_for_joins_under_memory_pressure`, the buckets of the two-level map switch to the Robin Hood
-- mode when growing them once more would make the join spill to disk.
-- 10 million right keys are about 39 thousand per bucket: 512 MiB of hash table in the ordinary mode, 256 MiB in the
-- Robin Hood mode, plus 153 MiB of right blocks. The join switches to `GraceHashJoin` at half of
-- `max_bytes_before_external_join`, 520 MiB, between the two.
SET join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', query_plan_join_swap_table = 0, max_threads = 2,
    enable_robin_hood_hash_for_joins = 0, enable_robin_hood_hash_for_joins_under_memory_pressure = 1, max_bytes_ratio_before_external_join = 0,
    collect_hash_table_stats_during_joins = 1, max_bytes_before_external_join = 1090519040;

SELECT count(), sum(r) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS log_comment = '05872_auto_cold';

-- The statistics of the first run reserve the hash tables at their final ordinary size, which already reaches the switch point, so the reserve is halved.
SELECT count(), sum(r) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS log_comment = '05872_auto_warm';

SELECT count(), sum(r) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS enable_robin_hood_hash_for_joins_under_memory_pressure = 0, log_comment = '05872_off';

-- Far from the threshold the mode is not used.
SELECT count(), sum(r) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS max_bytes_before_external_join = 100000000000, log_comment = '05872_auto_no_pressure';

-- Here the full reserve stays under the threshold, so it is not halved.
SELECT count(), sum(r) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS max_bytes_before_external_join = 2147483648, log_comment = '05872_auto_near';

SELECT count(), sum(r) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, number AS r FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS max_bytes_before_external_join = 100000000000, enable_robin_hood_hash_for_joins = 1, log_comment = '05872_always';

-- A `String` right column: the size statistics hold no bytes, so the first block gives the bytes of a row.
SELECT count(), sum(length(s)) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, toString(number + 1000000000) AS s FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS max_bytes_before_external_join = 100000000000, log_comment = '05872_string_seed';

SELECT count(), sum(length(s)) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, toString(number + 1000000000) AS s FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS max_bytes_before_external_join = 1444937728, log_comment = '05872_string_warm';

-- The same with a constant `String` column, which the join stores materialized.
SELECT count(), sum(length(s)) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, toString(1000000000) AS s FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS max_bytes_before_external_join = 100000000000, log_comment = '05872_const_seed';

SELECT count(), sum(length(s)) FROM numbers(1000000) AS l
INNER JOIN (SELECT number * 2 AS k, toString(1000000000) AS s FROM numbers(10000000)) AS rt ON l.number = rt.k
SETTINGS max_bytes_before_external_join = 1444937728, log_comment = '05872_const_warm';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'], ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05872_auto_warm', '05872_const_warm', '05872_off', '05872_string_warm')
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05872_auto_cold';

SELECT
    (SELECT max(memory_usage) FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND log_comment = '05872_auto_no_pressure')
    > 1.2 * (SELECT max(memory_usage) FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND log_comment = '05872_always');

SELECT log_comment, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'], ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05872_always', '05872_auto_near', '05872_auto_no_pressure')
ORDER BY log_comment;

-- The reserve is halved where the build would not grow it and would reach the threshold with it, and only there:
-- `05872_auto_warm` gets half of the 8.52 million keys that the threshold leaves of the 10 million of the earlier run.
-- `05872_string_warm` and `05872_const_warm` get half of the 10 million keys: their `String` column makes the join reach the threshold.
SELECT
    (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_auto_near')
    = (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_always'),
    (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_auto_warm') * 2
    < (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_always'),
    (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_string_warm') * 2
    <= (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_always'),
    (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_const_warm') * 2
    <= (SELECT max(ProfileEvents['HashJoinPreallocatedElementsInHashTables']) FROM system.query_log WHERE event_date >= yesterday()
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05872_always');
