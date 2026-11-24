-- A key of unknown length is hashed for every left row when packed, so a join without a runtime filter keeps exact `String`
-- keys when `min_rows_for_packed_string_keys_in_hash_join` is above the rows of the right side. Here the right side is a
-- `Memory` table of 1000000 rows, which has no statistics, and the optimizer uses its row count as the estimate. The table
-- is far above the L2 cache of any host, so only the minimum decides. The settings below keep this estimate and the build
-- side fixed, turn off the runtime filter, and turn off hash table statistics.
-- A join with packed keys counts the keys it inserts in `JoinPackedStringKeysInline` and `JoinPackedStringKeysHashed`.

SET enable_parallel_replicas = 0;
SET enable_packed_string_keys_in_hash_join = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 0;
-- Without a spill threshold, `hash` and `parallel_hash` do not spill. The `grace_hash` and spilling cases set one.
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

CREATE TABLE b (k String, v UInt64) ENGINE = Memory;
CREATE TABLE p (k String) ENGINE = Memory;
-- Half of the keys are stored inline, the other half as hashes.
INSERT INTO b SELECT if(number % 2 = 0, toString(number), concat('a key longer than 15 bytes ', toString(number))), number
FROM numbers(1000000);
-- The left side has 1000 of the keys, so that each join builds the large right table and probes little.
INSERT INTO p SELECT k FROM b WHERE v < 1000;

-- With the estimate of 1000000 rows, a minimum of 1000000 keeps packed keys and a minimum of 1000001 turns them off.
SELECT 'hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', min_rows_for_packed_string_keys_in_hash_join = 1000000, log_comment = '05463:hash:1000000';
SELECT 'hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', min_rows_for_packed_string_keys_in_hash_join = 1000001, log_comment = '05463:hash:1000001';
SELECT 'hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', min_rows_for_packed_string_keys_in_hash_join = 0, log_comment = '05463:hash:0';
SELECT 'hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', log_comment = '05463:hash:default';

SELECT 'parallel_hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 4,
    min_rows_for_packed_string_keys_in_hash_join = 1000000, log_comment = '05463:parallel_hash:1000000';
SELECT 'parallel_hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 4,
    min_rows_for_packed_string_keys_in_hash_join = 1000001, log_comment = '05463:parallel_hash:1000001';

SELECT 'grace_hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 2, max_bytes_before_external_join = '1Gi',
    min_rows_for_packed_string_keys_in_hash_join = 1000000, log_comment = '05463:grace_hash:1000000';
SELECT 'grace_hash', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'grace_hash', grace_hash_join_initial_buckets = 2, max_bytes_before_external_join = '1Gi',
    min_rows_for_packed_string_keys_in_hash_join = 1000001, log_comment = '05463:grace_hash:1000001';

-- A `hash` join with a spill threshold runs as a spilling join.
SELECT 'spilling', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = '1Gi',
    min_rows_for_packed_string_keys_in_hash_join = 1000000, log_comment = '05463:spilling:1000000';
SELECT 'spilling', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = '1Gi',
    min_rows_for_packed_string_keys_in_hash_join = 1000001, log_comment = '05463:spilling:1000001';

-- Without join reordering, the join takes this host's own estimate of its read of the right table, so it keeps exact keys.
-- For a right side read from `numbers`, there is no estimate, so the join uses packed keys.
SELECT 'own estimate', count() FROM p JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', query_plan_optimize_join_order_limit = 0,
    min_rows_for_packed_string_keys_in_hash_join = 1000001, log_comment = '05463:own_estimate:no_reordering';
SELECT 'no estimate', count()
FROM p JOIN
(
    SELECT if(number % 2 = 0, toString(number), concat('a key longer than 15 bytes ', toString(number))) AS k
    FROM numbers(1000000)
) AS n ON p.k = n.k
SETTINGS join_algorithm = 'hash', min_rows_for_packed_string_keys_in_hash_join = 1000001,
    log_comment = '05463:no_estimate:numbers';

-- The optimizer estimates the filtered right side at the 1000000 rows of `b`. The first run records its 500000 right rows in
-- the hash table statistics. The second run takes this count as the estimate.
SELECT 'statistics', count() FROM p JOIN (SELECT k FROM b WHERE v < 500000) AS f ON p.k = f.k
SETTINGS join_algorithm = 'hash', collect_hash_table_stats_during_joins = 1,
    min_rows_for_packed_string_keys_in_hash_join = 1000000, log_comment = '05463:statistics:first_run';
SELECT 'statistics', count() FROM p JOIN (SELECT k FROM b WHERE v < 500000) AS f ON p.k = f.k
SETTINGS join_algorithm = 'hash', collect_hash_table_stats_during_joins = 1,
    min_rows_for_packed_string_keys_in_hash_join = 1000000, log_comment = '05463:statistics:second_run';

SYSTEM FLUSH LOGS query_log;

SELECT replaceOne(log_comment, '05463:', '') AS config,
    ProfileEvents['JoinPackedStringKeysInline'] > 0 AS inline_keys, ProfileEvents['JoinPackedStringKeysHashed'] > 0 AS hashed_keys
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND startsWith(log_comment, '05463:')
ORDER BY event_time_microseconds;
