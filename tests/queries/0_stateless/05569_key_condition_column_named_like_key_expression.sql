-- Tags: no-replicated-database
-- A column named like an expression of the partition key is not that expression, so a filter on it must not prune by the key.

DROP TABLE IF EXISTS t_collision_expr;
CREATE TABLE t_collision_expr (d Date, `toYYYYMM(d)` UInt32, b UInt64)
ENGINE = MergeTree ORDER BY b PARTITION BY toYYYYMM(d) SETTINGS index_granularity = 1;
INSERT INTO t_collision_expr VALUES ('2024-01-01', 7, 10);

SELECT d, `toYYYYMM(d)`, b FROM t_collision_expr WHERE `toYYYYMM(d)` = 7;
SELECT d, `toYYYYMM(d)`, b FROM t_collision_expr WHERE (7 = `toYYYYMM(d)`) AND (b = toNullable(10)) ORDER BY b;
SELECT count() FROM t_collision_expr WHERE `toYYYYMM(d)` = 7;
SELECT count() FROM t_collision_expr WHERE `toYYYYMM(d)` + 1 = 8;
SELECT count() FROM t_collision_expr WHERE `toYYYYMM(d)` IN (7, 8);
SELECT count() FROM t_collision_expr WHERE indexHint(`toYYYYMM(d)` = 7);
SELECT count() FROM t_collision_expr WHERE toYYYYMM(d) = 202401;
DROP TABLE t_collision_expr;

-- The column is named like a subexpression of the partition key.
DROP TABLE IF EXISTS t_collision_subexpr;
CREATE TABLE t_collision_subexpr (d Date, `toYYYYMM(d)` UInt32)
ENGINE = MergeTree ORDER BY tuple() PARTITION BY intDiv(toYYYYMM(d), 100);
INSERT INTO t_collision_subexpr VALUES ('2024-01-01', 7);
SELECT count() FROM t_collision_subexpr WHERE `toYYYYMM(d)` = 7;
DROP TABLE t_collision_subexpr;

-- Partition pruning of a mutation.
DROP TABLE IF EXISTS t_collision_rmt;
CREATE TABLE t_collision_rmt (d Date, `toYYYYMM(d)` UInt32, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_collision_rmt', '1') PARTITION BY toYYYYMM(d) ORDER BY b;
INSERT INTO t_collision_rmt VALUES ('2024-01-01', 7, 10);
ALTER TABLE t_collision_rmt DELETE WHERE `toYYYYMM(d)` = 7 SETTINGS mutations_sync = 2;
SELECT count() FROM t_collision_rmt;
DROP TABLE t_collision_rmt;
