-- `GROUP BY ... WITH CLUSTER` over a direct `MergeTree` read with custom-key parallel replicas.
-- The custom key splits adjacent keys between the replicas, so the clustering must run once,
-- on the initiator, over the groups of all replicas instead of on each replica separately.

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 0;
SET parallel_replicas_custom_key = 'x';
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_cluster_custom_key;
CREATE TABLE t_cluster_custom_key (x UInt8, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_cluster_custom_key VALUES (0, 'aaaa'), (1, 'aaab'), (2, 'aabb'), (10, 'zzzz'), (11, 'zzzy'), (20, 'qqqqqqqq');

SELECT '-- sampling, 1D';
SELECT min(x), max(x), count(), sum(x) FROM t_cluster_custom_key GROUP BY x WITH CLUSTER 1 ORDER BY min(x)
SETTINGS parallel_replicas_mode = 'custom_key_sampling';

SELECT '-- range, 1D';
SELECT min(x), max(x), count(), sum(x) FROM t_cluster_custom_key GROUP BY x WITH CLUSTER 1 ORDER BY min(x)
SETTINGS parallel_replicas_mode = 'custom_key_range';

SELECT '-- sampling, 1D, HAVING, ORDER BY, LIMIT';
SELECT min(x), count() AS c FROM t_cluster_custom_key GROUP BY x WITH CLUSTER 1 HAVING c > 1 ORDER BY c DESC LIMIT 1
SETTINGS parallel_replicas_mode = 'custom_key_sampling';

SELECT '-- sampling, strings';
SELECT min(s), max(s), count() FROM t_cluster_custom_key GROUP BY s WITH CLUSTER 1 ORDER BY min(s)
SETTINGS parallel_replicas_mode = 'custom_key_sampling';

SELECT '-- sampling, 2D';
SELECT min(x), max(x), count() FROM t_cluster_custom_key GROUP BY (x, x) WITH CLUSTER 1.5 ORDER BY min(x)
SETTINGS parallel_replicas_mode = 'custom_key_sampling';

SELECT '-- without parallel replicas';
SELECT min(x), max(x), count(), sum(x) FROM t_cluster_custom_key GROUP BY x WITH CLUSTER 1 ORDER BY min(x)
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_cluster_custom_key;
