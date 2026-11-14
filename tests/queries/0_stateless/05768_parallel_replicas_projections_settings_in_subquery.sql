-- Random settings limits: optimize_use_projections=(1, None); optimize_use_implicit_projections=(1, None); parallel_replicas_local_plan=(1, None); automatic_parallel_replicas_mode=(0, 0); materialize_statistics_on_insert=(1, None)

-- Parallel replicas enabled only in a subquery's SETTINGS: what a projection, the minmax / exact count or column statistics answer must still be returned.

DROP TABLE IF EXISTS t_pr_subquery;
CREATE TABLE t_pr_subquery (x UInt64, b UInt64) ENGINE = MergeTree ORDER BY x SETTINGS auto_statistics_types = 'basic';
SYSTEM STOP MERGES t_pr_subquery;
INSERT INTO t_pr_subquery SELECT number, number % 100 FROM numbers(200000) SETTINGS max_insert_threads = 1;
ALTER TABLE t_pr_subquery ADD PROJECTION agg (SELECT b, sum(x) GROUP BY b), ADD PROJECTION p (SELECT * ORDER BY b);
INSERT INTO t_pr_subquery SELECT 200000 + number, number % 100 FROM numbers(100000) SETTINGS max_insert_threads = 1;

SET enable_parallel_replicas = 0, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT 'minmax', * FROM (SELECT min(x), max(x), count() FROM t_pr_subquery SETTINGS enable_parallel_replicas = 1) SETTINGS log_comment = '05768_minmax';
SELECT 'statistics', * FROM (SELECT sum(x), avg(x) FROM t_pr_subquery SETTINGS enable_parallel_replicas = 1) SETTINGS log_comment = '05768_statistics';
SELECT 'exact count', * FROM (SELECT count() FROM t_pr_subquery WHERE x < 70000 SETTINGS enable_parallel_replicas = 1) SETTINGS log_comment = '05768_exact_count';
SELECT 'projection beside parts without it', * FROM (SELECT b, sum(x) FROM t_pr_subquery WHERE b < 3 GROUP BY b SETTINGS enable_parallel_replicas = 1) ORDER BY b SETTINGS log_comment = '05768_agg_proj';
SELECT 'normal projection beside parts without it', * FROM (SELECT count(), sum(x) FROM t_pr_subquery WHERE b = 7 SETTINGS enable_parallel_replicas = 1) SETTINGS log_comment = '05768_normal_proj';

SYSTEM FLUSH LOGS query_log;
SELECT 'served by', log_comment, min(ProfileEvents['ParallelReplicasQueryCount'] > 0),
    arraySort(groupUniqArrayArray(arrayMap(p -> arrayStringConcat(arrayPopFront(splitByChar('.', p)), '.'), projections)))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('05768_minmax', '05768_statistics', '05768_exact_count', '05768_agg_proj', '05768_normal_proj')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_pr_subquery;
