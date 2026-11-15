-- Tags: no-fasttest
-- no-fasttest: needs the static stateless worker cluster from the distributed query test configuration.

-- `distributed_plan_workers_num` on a static stateless worker cluster plans for, and runs on, exactly
-- that many hosts: fewer than the cluster size, the whole cluster, or an exception when it asks for
-- more hosts than the cluster has.

DROP TABLE IF EXISTS t_static_workers_num;
CREATE TABLE t_static_workers_num (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_static_workers_num SELECT number FROM numbers(1000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0;

-- `test_cluster_one_shard_two_replicas`, the static stateless worker cluster configured for this test
-- suite, has two hosts.
SELECT 'fewer workers than hosts';
SELECT count() FROM t_static_workers_num SETTINGS distributed_plan_workers_num = 1;

SELECT 'as many workers as hosts';
SELECT count() FROM t_static_workers_num SETTINGS distributed_plan_workers_num = 2;

SELECT 'more workers than hosts';
SELECT count() FROM t_static_workers_num SETTINGS distributed_plan_workers_num = 3; -- { serverError SW_SERVER_NO_WORKERS_AVAILABLE }

SET make_distributed_plan = 0;
DROP TABLE t_static_workers_num;
