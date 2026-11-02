-- Random settings limits: parallel_replicas_local_plan=(1, 1); query_plan_optimize_join_order_randomize=(0, 0)
-- A constant-false ON conjunct replaces the non-preserved side of a RIGHT JOIN with an empty source.
-- Plan-based parallel replicas optimize the local plan again after join runtime filters were added,
-- and every join algorithm must still get a usable header from that empty source.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (b String) ENGINE = MergeTree ORDER BY b;
CREATE TABLE t_right (b String) ENGINE = MergeTree ORDER BY b;

INSERT INTO t_left VALUES ('a'), ('b');
INSERT INTO t_right VALUES ('a'), ('c');

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET enable_join_runtime_filters = 1;
SET query_plan_short_circuit_constant_false_join = 1;
SET max_bytes_before_external_join = 1000000000;

SELECT 'hash RIGHT', count() FROM t_left AS l RIGHT JOIN t_right AS r ON l.b = r.b AND CAST(NULL AS Nullable(UInt8)) SETTINGS join_algorithm = 'hash';
-- The in-memory join switches to grace_hash at the first block.
SELECT 'parallel_hash RIGHT spill', count() FROM t_left AS l RIGHT JOIN t_right AS r ON l.b = r.b AND CAST(NULL AS Nullable(UInt8)) SETTINGS join_algorithm = 'parallel_hash', max_bytes_before_external_join = 1;
SELECT 'grace_hash RIGHT', count() FROM t_left AS l RIGHT JOIN t_right AS r ON l.b = r.b AND CAST(NULL AS Nullable(UInt8)) SETTINGS join_algorithm = 'grace_hash';
SELECT 'join_use_nulls', l.b, r.b FROM t_left AS l RIGHT JOIN t_right AS r ON l.b = r.b AND CAST(NULL AS Nullable(UInt8)) ORDER BY r.b SETTINGS join_algorithm = 'hash', join_use_nulls = 1;

DROP TABLE t_left;
DROP TABLE t_right;
