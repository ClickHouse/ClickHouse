-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: the test uses explicit ZooKeeper paths in the engine arguments.
-- Tag no-shared-merge-tree: same reason.

-- The physical columns of a projection part are named after the parameters of its `APPLY`
-- transformer as written (`quantile(ABS(-0.9))(b)` versus `quantile(abs(-0.9))(b)`), so a replica
-- whose projection differs only by that spelling must not join the table: its parts would carry
-- projection columns the other replicas do not know. The same spelling still joins and replicates.

DROP TABLE IF EXISTS t_apply_r1_05401;
DROP TABLE IF EXISTS t_apply_r2_05401;

CREATE TABLE t_apply_r1_05401 (a UInt32, b Float64,
    PROJECTION p (SELECT a, COLUMNS('b') APPLY quantile(abs(-0.9)) GROUP BY a))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_apply_05401', 'r1') ORDER BY a;

CREATE TABLE t_apply_r2_05401 (a UInt32, b Float64,
    PROJECTION p (SELECT a, COLUMNS('b') APPLY quantile(ABS(-0.9)) GROUP BY a))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_apply_05401', 'r2') ORDER BY a; -- { serverError METADATA_MISMATCH }

CREATE TABLE t_apply_r2_05401 (a UInt32, b Float64,
    PROJECTION p (SELECT a, COLUMNS('b') APPLY quantile(abs(-0.9)) GROUP BY a))
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_apply_05401', 'r2') ORDER BY a;

INSERT INTO t_apply_r2_05401 SELECT number % 10, number % 10 + 1 FROM numbers(100);
SYSTEM SYNC REPLICA t_apply_r1_05401;

-- Forced projection reads agree with base reads on the replica that did not write the part.
SELECT a, quantile(abs(-0.9))(b) FROM t_apply_r1_05401 GROUP BY a ORDER BY a SETTINGS optimize_use_projections = 0;
SELECT a, quantile(abs(-0.9))(b) FROM t_apply_r1_05401 GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

DROP TABLE t_apply_r1_05401;
DROP TABLE t_apply_r2_05401;
