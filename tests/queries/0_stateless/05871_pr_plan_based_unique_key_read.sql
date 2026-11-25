-- Tags: no-fasttest, no-ordinary-database, no-shared-merge-tree
-- Tag no-fasttest: a UNIQUE KEY INSERT writes the dense-index SST, which needs RocksDB.
-- Tag no-ordinary-database, no-shared-merge-tree: UNIQUE KEY tables need an Atomic database and a plain MergeTree.

-- Under plan-based parallel replicas (`parallel_replicas_plan_based`), a read of a UNIQUE KEY table cannot be shipped to
-- the replicas, so it stays on the initiator, as with query-based parallel replicas, while the rest of the query can
-- still be distributed. https://github.com/ClickHouse/ClickHouse/issues/128534

SET enable_unique_key = 1;

DROP TABLE IF EXISTS t_pr_uk;
DROP TABLE IF EXISTS t_pr_plain;

CREATE TABLE t_pr_uk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k UNIQUE KEY (k);
CREATE TABLE t_pr_plain (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_uk SELECT number, number FROM numbers(1000);
INSERT INTO t_pr_plain SELECT number FROM numbers(2000);

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
-- Without the local plan every replica is a remote one, so a shipped read would always reach one.
SET parallel_replicas_local_plan = 0;

SELECT count(), sum(v) FROM t_pr_uk;
-- The UNIQUE KEY table on either side of a join.
SELECT count(), sum(v) FROM t_pr_plain AS p JOIN t_pr_uk AS u USING (k);
SELECT count(), sum(v) FROM t_pr_uk AS u JOIN t_pr_plain AS p USING (k);
-- The UNIQUE KEY table in an IN set.
SELECT count() FROM t_pr_plain WHERE k IN (SELECT k FROM t_pr_uk);

DROP TABLE t_pr_uk;
DROP TABLE t_pr_plain;
