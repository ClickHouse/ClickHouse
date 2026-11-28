-- Tags: no-replicated-database
-- A mutation whose predicate names both the partition key expression and a column named like it must not skip the partition.
-- https://github.com/ClickHouse/ClickHouse/issues/130256

DROP TABLE IF EXISTS t_collision_rmt_expr;
CREATE TABLE t_collision_rmt_expr (d Date, `toYYYYMM(d)` UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_collision_rmt_expr', '1') PARTITION BY toYYYYMM(d) ORDER BY tuple();

INSERT INTO t_collision_rmt_expr VALUES ('2024-01-01', 7);
ALTER TABLE t_collision_rmt_expr DELETE WHERE toYYYYMM(d) = 202401 AND `toYYYYMM(d)` = 7
SETTINGS mutations_sync = 2, optimize_mutations_with_partition_pruning = 0;
SELECT count() FROM t_collision_rmt_expr;

INSERT INTO t_collision_rmt_expr VALUES ('2024-01-02', 7);
ALTER TABLE t_collision_rmt_expr DELETE WHERE toYYYYMM(d) = 202401 AND `toYYYYMM(d)` = 7
SETTINGS mutations_sync = 2, optimize_mutations_with_partition_pruning = 1;
SELECT count() FROM t_collision_rmt_expr;

DROP TABLE t_collision_rmt_expr;

-- The partition key does not compute the expression (`toYYYYMM` is a monotonic function of `d`), the collision is still in the predicate.
DROP TABLE IF EXISTS t_collision_rmt_col;
CREATE TABLE t_collision_rmt_col (d Date, `toYYYYMM(d)` UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_collision_rmt_col', '1') PARTITION BY d ORDER BY tuple();
INSERT INTO t_collision_rmt_col VALUES ('2024-01-01', 7);
ALTER TABLE t_collision_rmt_col DELETE WHERE toYYYYMM(d) = 202401 AND `toYYYYMM(d)` = 7
SETTINGS mutations_sync = 2, optimize_mutations_with_partition_pruning = 1;
SELECT count() FROM t_collision_rmt_col;
DROP TABLE t_collision_rmt_col;

-- The column and the function have different types.
DROP TABLE IF EXISTS t_collision_rmt_type;
CREATE TABLE t_collision_rmt_type (d Date, `toYYYYMM(d)` String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_collision_rmt_type', '1') PARTITION BY d ORDER BY tuple();
INSERT INTO t_collision_rmt_type VALUES ('2024-01-01', '7');
ALTER TABLE t_collision_rmt_type DELETE WHERE toString(toYYYYMM(d)) = '202401' AND length(`toYYYYMM(d)`) = 1
SETTINGS mutations_sync = 2, optimize_mutations_with_partition_pruning = 1;
SELECT count() FROM t_collision_rmt_type;
DROP TABLE t_collision_rmt_type;
