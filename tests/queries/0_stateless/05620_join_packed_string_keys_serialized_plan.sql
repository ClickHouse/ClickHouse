-- Tags: distributed

-- The shards run the join below from the serialized query plan. With `enable_packed_string_keys_in_hash_join`, their
-- join on a single `String` key counts the inline keys in `JoinPackedStringKeysInline` and the hashed ones in
-- `JoinPackedStringKeysHashed`. A shard may have no row estimate, so `min_rows_for_packed_string_keys_in_hash_join` is 0.

CREATE TABLE t_left (k String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_right (k String) ENGINE = MergeTree ORDER BY k;
-- Half of the keys are stored inline, the other half as hashes.
INSERT INTO t_left SELECT if(number % 2 = 0, toString(number), concat('a key longer than 15 bytes ', toString(number))) FROM numbers(1000);
INSERT INTO t_right SELECT k FROM t_left;

SELECT count()
FROM cluster('test_cluster_two_shards', currentDatabase(), t_left) AS l
INNER JOIN cluster('test_cluster_two_shards', currentDatabase(), t_right) AS r ON l.k = r.k
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0, join_algorithm = 'hash',
    enable_packed_string_keys_in_hash_join = 1, min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05620_on';

SELECT count()
FROM cluster('test_cluster_two_shards', currentDatabase(), t_left) AS l
INNER JOIN cluster('test_cluster_two_shards', currentDatabase(), t_right) AS r ON l.k = r.k
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0, join_algorithm = 'hash',
    enable_packed_string_keys_in_hash_join = 0, min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05620_off';

-- The tasks of a distributed query plan get an already optimized join and no row estimate. The 1000-row right side is
-- below `min_rows_for_packed_string_keys_in_hash_join`, so they compare the keys exactly, as a local join does.
SELECT count()
FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    enable_parallel_replicas = 0, max_rows_to_group_by = 0, join_algorithm = 'hash',
    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
    enable_packed_string_keys_in_hash_join = 1, min_rows_for_packed_string_keys_in_hash_join = 100000, log_comment = '05620_plan_small';

SELECT count()
FROM t_left AS l INNER JOIN t_right AS r ON l.k = r.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    enable_parallel_replicas = 0, max_rows_to_group_by = 0, join_algorithm = 'hash',
    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
    enable_packed_string_keys_in_hash_join = 1, min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05620_plan_no_min';

SYSTEM FLUSH LOGS query_log;

-- The remote queries of each run: at least one, and the keys their joins inserted packed.
SELECT log_comment, count() > 0, sum(ProfileEvents['JoinPackedStringKeysInline']) > 0, sum(ProfileEvents['JoinPackedStringKeysHashed']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND current_database = currentDatabase()
            AND log_comment IN ('05620_on', '05620_off', '05620_plan_small', '05620_plan_no_min'))
GROUP BY log_comment
ORDER BY log_comment;
