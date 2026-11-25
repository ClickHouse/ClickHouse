-- grouping() with ROLLUP, CUBE and GROUPING SETS in a subquery of INSERT SELECT executed with parallel replicas.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_05814_src;
DROP TABLE IF EXISTS t_05814_dst;

CREATE TABLE t_05814_src (k UInt64, a UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05814_src SELECT number, number % 3 FROM numbers(30);
CREATE TABLE t_05814_dst (k UInt64, a UInt8) ENGINE = Memory;

SET parallel_distributed_insert_select = 2, enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

INSERT INTO t_05814_dst SELECT * FROM t_05814_src WHERE a IN (SELECT a FROM t_05814_src GROUP BY a HAVING grouping(a) = 0 AND a < 2);
SELECT 'ordinary', count() FROM t_05814_dst;
TRUNCATE TABLE t_05814_dst;

INSERT INTO t_05814_dst SELECT * FROM t_05814_src WHERE a IN (SELECT a FROM t_05814_src GROUP BY ROLLUP(a) HAVING grouping(a) = 0 AND a < 2);
SELECT 'ROLLUP', count() FROM t_05814_dst;
TRUNCATE TABLE t_05814_dst;

INSERT INTO t_05814_dst SELECT * FROM t_05814_src WHERE a IN (SELECT a FROM t_05814_src GROUP BY CUBE(a) HAVING grouping(a) = 0 AND a < 2);
SELECT 'CUBE', count() FROM t_05814_dst;
TRUNCATE TABLE t_05814_dst;

INSERT INTO t_05814_dst SELECT * FROM t_05814_src WHERE a IN (SELECT a FROM t_05814_src GROUP BY GROUPING SETS ((a), ()) HAVING grouping(a) = 0 AND a < 2);
SELECT 'GROUPING SETS', count() FROM t_05814_dst;

DROP TABLE t_05814_src;
DROP TABLE t_05814_dst;
