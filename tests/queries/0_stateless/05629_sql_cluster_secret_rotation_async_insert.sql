-- Tags: no-parallel, no-fasttest
-- no-parallel: creates and drops a global SQL-managed cluster
-- no-fasttest: Fast test builds ClickHouse without SSL, so inter-server `secret` is disabled

-- The connection pool of an async `Distributed` insert queue signs every request with the cluster `secret`.
-- `ALTER CLUSTER` that changes only the secret must recreate the queue, otherwise it keeps signing with the old
-- secret, while the receiving server checks the new one, and the queued blocks are never delivered.

DROP TABLE IF EXISTS dist_05629;
DROP TABLE IF EXISTS data_05629;
DROP CLUSTER IF EXISTS test_sql_cluster_05629;

CREATE CLUSTER test_sql_cluster_05629 (secret = 'secret_05629_1', SHARD (host = '127.0.0.2', port = 9000));

CREATE TABLE data_05629 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_05629 AS data_05629 ENGINE = Distributed(test_sql_cluster_05629, currentDatabase(), data_05629);

SET distributed_foreground_insert = 0, prefer_localhost_replica = 0;

INSERT INTO dist_05629 VALUES (1);
SYSTEM FLUSH DISTRIBUTED dist_05629;
SELECT count() FROM data_05629;

ALTER CLUSTER test_sql_cluster_05629 (secret = 'secret_05629_2', SHARD (host = '127.0.0.2', port = 9000));

INSERT INTO dist_05629 VALUES (2);
SYSTEM FLUSH DISTRIBUTED dist_05629;
SELECT count() FROM data_05629;

DROP TABLE dist_05629;
DROP TABLE data_05629;
DROP CLUSTER test_sql_cluster_05629;
