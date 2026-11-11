-- Tags: distributed

-- The shards run the join below from the serialized query plan. With `enable_packed_string_keys_in_hash_join`, their
-- join on a single `String` key counts the inline keys in `JoinPackedStringKeysInline` and the hashed ones in
-- `JoinPackedStringKeysHashed`. A shard may estimate the right side at fewer rows than the default minimum.

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

SYSTEM FLUSH LOGS query_log;

-- The remote queries of each run: at least one, and the keys their joins inserted packed.
SELECT log_comment, count() > 0, sum(ProfileEvents['JoinPackedStringKeysInline']) > 0, sum(ProfileEvents['JoinPackedStringKeysHashed']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND current_database = currentDatabase() AND log_comment IN ('05620_on', '05620_off'))
GROUP BY log_comment
ORDER BY log_comment;
