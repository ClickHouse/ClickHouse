-- A row policy on a `MergeTree` table, which applies the policy inside the reading step,
-- must be applied to a read shipped as a serialized query plan.

SET serialize_query_plan = 1, prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_05622;
CREATE TABLE t_05622 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05622 SELECT number, number FROM numbers(10);

DROP ROW POLICY IF EXISTS policy_05622 ON t_05622;
CREATE ROW POLICY policy_05622 ON t_05622 FOR SELECT USING y < 5 TO CURRENT_USER;

SELECT 'the query does not read the policy column';
SELECT count() FROM cluster('test_shard_localhost', currentDatabase(), t_05622);

SELECT 'the query reads the policy column';
SELECT DISTINCT y FROM cluster('test_shard_localhost', currentDatabase(), t_05622) ORDER BY y;

SELECT 'Distributed table';
DROP TABLE IF EXISTS t_dist_05622;
CREATE TABLE t_dist_05622 AS t_05622 ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_05622);
SELECT sum(x) FROM t_dist_05622 WHERE y > 2;

SELECT 'the policy and FINAL';
DROP TABLE IF EXISTS t_final_05622;
CREATE TABLE t_final_05622 (x UInt64, y UInt64) ENGINE = ReplacingMergeTree ORDER BY x;
SYSTEM STOP MERGES t_final_05622;
INSERT INTO t_final_05622 VALUES (1, 1);
INSERT INTO t_final_05622 VALUES (1, 9);

DROP ROW POLICY IF EXISTS policy_final_05622 ON t_final_05622;
CREATE ROW POLICY policy_final_05622 ON t_final_05622 FOR SELECT USING y < 5 TO CURRENT_USER;

SELECT x, y FROM cluster('test_shard_localhost', currentDatabase(), t_final_05622) FINAL SETTINGS apply_row_policy_after_final = 0;
SELECT x, y FROM cluster('test_shard_localhost', currentDatabase(), t_final_05622) FINAL SETTINGS apply_row_policy_after_final = 1;

SELECT 'parallel replicas choose the same reading mode';
DROP TABLE IF EXISTS t_key_05622;
CREATE TABLE t_key_05622 (a String, b UInt64) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1;
INSERT INTO t_key_05622 VALUES ('engineering', 1), ('finance', 2), ('engineering', 3), ('hr', 4);

DROP ROW POLICY IF EXISTS policy_key_05622 ON t_key_05622;
CREATE ROW POLICY policy_key_05622 ON t_key_05622 FOR SELECT USING a = 'nomatch' TO CURRENT_USER;

SELECT a, b FROM t_key_05622 ORDER BY b
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 0;

DROP ROW POLICY policy_key_05622 ON t_key_05622;
DROP ROW POLICY policy_final_05622 ON t_final_05622;
DROP ROW POLICY policy_05622 ON t_05622;
DROP TABLE t_key_05622;
DROP TABLE t_final_05622;
DROP TABLE t_dist_05622;
DROP TABLE t_05622;
