-- A non-correlated EXISTS that only filters the rows of a subquery whose result an outer LIMIT 0 discards
-- is not executed during analysis with execute_exists_as_scalar_subquery = 1.

DROP TABLE IF EXISTS t;
CREATE TABLE t (c1 UInt64, c2 UInt64) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t SELECT number, number % 30000 FROM numbers(100000);
DROP TABLE IF EXISTS t_dist;
CREATE TABLE t_dist AS t ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t);

SET execute_exists_as_scalar_subquery = 1, exact_rows_before_limit = 0;

SELECT * FROM (WITH s AS (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1) SELECT k, c FROM s UNION ALL SELECT 0 AS k, 0 AS c WHERE NOT exists((SELECT 1 FROM s))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_issue' FORMAT TSVWithNamesAndTypes;

SELECT * FROM (WITH p AS (SELECT toDate('2026-01-01') AS c9), s AS (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1), u AS (SELECT k, c FROM s UNION ALL SELECT k, c FROM s WHERE k < 10) SELECT p.c9 AS d, k, c FROM u CROSS JOIN p UNION ALL SELECT p.c9 AS d, 0 AS k, 0 AS c FROM p WHERE NOT exists((SELECT 1 FROM u))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_issue_cross_join';

SELECT * FROM (SELECT number AS x FROM numbers(10)) AS a INNER JOIN (SELECT c1 AS x FROM t WHERE NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) AS b USING (x) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_join';

SELECT * FROM (SELECT c1 % 10 AS g, count() FROM t GROUP BY g HAVING NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_having';

SELECT * FROM (SELECT c1, row_number() OVER () AS rn FROM t QUALIFY NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_qualify';

-- The EXISTS is executed during analysis in the cases below.

SELECT count() FROM (WITH s AS (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1) SELECT k, c FROM s UNION ALL SELECT 0 AS k, 0 AS c WHERE NOT exists((SELECT 1 FROM s)))
SETTINGS log_comment = 'exists_limit_zero_no_limit_zero';

SELECT c1 FROM t WHERE NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_own_limit_zero';

SELECT * FROM (SELECT c1 FROM t PREWHERE NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_prewhere';

SELECT * FROM (SELECT toDecimal32(1, exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) AS d) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_constant_argument' FORMAT TSVWithNamesAndTypes;

SELECT * FROM (SELECT count() FROM t WHERE (exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1))) AS e) LIMIT e) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_aliased_filter';

WITH x -> toDecimal32(1, x) > 0 AS `not` SELECT * FROM (SELECT 1 AS a WHERE NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_shadowed_not' FORMAT TSVWithNamesAndTypes;

SELECT (SELECT count() FROM t WHERE NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) AS x LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_scalar_subquery';

SELECT * FROM (SELECT c1 % 10 AS g, count() AS n FROM t WHERE NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1))) GROUP BY g WITH TOTALS) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_totals';

SELECT * FROM (WITH s AS (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1) SELECT k, c FROM s UNION ALL SELECT 0 AS k, 0 AS c WHERE NOT exists((SELECT 1 FROM s))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_exact_rows', exact_rows_before_limit = 1;

SELECT * FROM (SELECT c1 FROM t_dist WHERE NOT exists((SELECT 1 FROM t_dist))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_distributed', distributed_product_mode = 'deny';

SELECT * FROM (SELECT c1 FROM t WHERE NOT exists((SELECT 1 FROM (SELECT c2 AS k, count() AS c FROM t GROUP BY k HAVING c > 1)))) LIMIT 0
SETTINGS log_comment = 'exists_limit_zero_parallel_replicas', enable_parallel_replicas = 2, parallel_replicas_allow_in_with_subquery = 0,
max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ScalarSubqueriesCacheMiss'], read_rows > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE 'exists_limit_zero_%'
ORDER BY log_comment;

DROP TABLE t_dist;
DROP TABLE t;
