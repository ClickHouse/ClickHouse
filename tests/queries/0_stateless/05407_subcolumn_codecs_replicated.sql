-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the replicas are created with explicit ZooKeeper paths.
-- no-shared-merge-tree: the test reads the `columns` node of ReplicatedMergeTree.

SET enable_subcolumn_codecs = 1;
SET alter_sync = 2;

DROP TABLE IF EXISTS t_subcolumn_codecs_r1;
DROP TABLE IF EXISTS t_subcolumn_codecs_r2;

CREATE TABLE t_subcolumn_codecs_r1 (id UInt64, p Tuple(a UInt64, b Float64), CODEC p.a (Delta, ZSTD(1)))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_subcolumn_codecs', 'r1') ORDER BY id;
CREATE TABLE t_subcolumn_codecs_r2 (id UInt64, p Tuple(a UInt64, b Float64), CODEC p.a (Delta, ZSTD(1)))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_subcolumn_codecs', 'r2') ORDER BY id;

-- A replica with other declarations has a different structure.
CREATE TABLE t_subcolumn_codecs_r3 (id UInt64, p Tuple(a UInt64, b Float64), CODEC p.a (LZ4))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_subcolumn_codecs', 'r3') ORDER BY id; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT 'columns node';
SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t_subcolumn_codecs' AND name = 'columns' FORMAT TSVRaw;

ALTER TABLE t_subcolumn_codecs_r1 MODIFY COLUMN p.b CODEC(Gorilla, LZ4);
SYSTEM SYNC REPLICA t_subcolumn_codecs_r2;

SELECT 'after alter';
SELECT table, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table LIKE 't_subcolumn_codecs_r%' AND name = 'p' ORDER BY table;
SELECT value FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t_subcolumn_codecs' AND name = 'columns' FORMAT TSVRaw;

INSERT INTO t_subcolumn_codecs_r2 SELECT number, (number, number / 7) FROM numbers(100);
SYSTEM SYNC REPLICA t_subcolumn_codecs_r1;
SELECT count(), sum(p.a) FROM t_subcolumn_codecs_r1;

DETACH TABLE t_subcolumn_codecs_r2;
ATTACH TABLE t_subcolumn_codecs_r2;
SELECT 'after attach';
SELECT table, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_r2' AND name = 'p';

DROP TABLE t_subcolumn_codecs_r1 SYNC;
DROP TABLE t_subcolumn_codecs_r2 SYNC;
