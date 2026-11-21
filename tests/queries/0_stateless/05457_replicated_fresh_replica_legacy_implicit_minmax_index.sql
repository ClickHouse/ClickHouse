-- Tags: zookeeper, no-random-merge-tree-settings, no-shared-merge-tree, no-replicated-database
-- Tag no-random-merge-tree-settings: the test relies on the default implicit-index policy and lists the skip indices.
-- Tag no-shared-merge-tree: the test covers the `ReplicatedMergeTree` metadata check against ZooKeeper.
-- Tag no-replicated-database: the replicas of one table are created in the same database.

-- Before 25.12, a table that inherited `add_minmax_index_for_numeric_columns = 1` from the server config
-- stored its implicit index in ZooKeeper as `auto_minmax_index_x x TYPE minmax() GRANULARITY 1`, while its
-- original `CREATE TABLE` had neither the index nor the setting. A new replica created from that original
-- definition generates the same index as an implicit one, so the ZooKeeper declaration must be normalized
-- away, otherwise the strict comparison fails with `METADATA_MISMATCH` and the replica cannot be created.
-- The first replica declares the index explicitly, with the same `minmax()` spelling, to leave exactly that
-- declaration in ZooKeeper.

DROP TABLE IF EXISTS t_05457_r1 SYNC;
DROP TABLE IF EXISTS t_05457_r2 SYNC;

CREATE TABLE t_05457_r1 (x UInt64, INDEX auto_minmax_index_x x TYPE minmax() GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05457', 'r1') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_05457_r1 SELECT number FROM numbers(10);

-- The original index-free definition, inheriting the implicit-index policy.
CREATE TABLE t_05457_r2 (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05457', 'r2') ORDER BY tuple();

SELECT table, name, creation FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table LIKE 't_05457_%' ORDER BY table, name;

SYSTEM SYNC REPLICA t_05457_r2;
SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 't_05457_r2';
SELECT count() FROM t_05457_r2 WHERE x = 5;

DROP TABLE t_05457_r2 SYNC;
DROP TABLE t_05457_r1 SYNC;
