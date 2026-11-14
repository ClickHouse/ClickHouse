-- FINAL must not prune parts by the min-max index of a partition key column that is not in the sorting key:
-- the versions of a row merged within one partition can have different values of it.

DROP TABLE IF EXISTS t_final_minmax_replacing;
DROP TABLE IF EXISTS t_final_minmax_collapsing;
DROP TABLE IF EXISTS t_final_minmax_dnm;
DROP TABLE IF EXISTS t_final_float_partition;
DROP TABLE IF EXISTS t_final_name_collision;
DROP TABLE IF EXISTS t_final_lambda;

CREATE TABLE t_final_minmax_replacing (k UInt64, d Date, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY toYYYYMM(d) ORDER BY (toYYYYMM(d), k);
SYSTEM STOP MERGES t_final_minmax_replacing;
INSERT INTO t_final_minmax_replacing VALUES (1, '2026-01-01', 1);
INSERT INTO t_final_minmax_replacing VALUES (1, '2026-01-02', 2);
INSERT INTO t_final_minmax_replacing VALUES (2, '2026-02-01', 1);

SELECT 'replacing';
SELECT * FROM t_final_minmax_replacing FINAL WHERE d = '2026-01-01';
SELECT * FROM t_final_minmax_replacing FINAL WHERE d = '2026-01-02';
SELECT count() FROM t_final_minmax_replacing FINAL WHERE d < '2026-01-02';
-- The partition expression is in the sorting key, so pruning by partition value still skips the other month.
SELECT match(explain, '"Type": "Partition",[^}]*"Selected Parts": 2,') FROM (EXPLAIN indexes = 1, json = 1 SELECT * FROM t_final_minmax_replacing FINAL WHERE d = '2026-01-01');

CREATE TABLE t_final_minmax_collapsing (k UInt64, d Date, sign Int8)
ENGINE = CollapsingMergeTree(sign) PARTITION BY toYYYYMM(d) ORDER BY (toYYYYMM(d), k);
SYSTEM STOP MERGES t_final_minmax_collapsing;
INSERT INTO t_final_minmax_collapsing VALUES (1, '2026-01-01', 1);
INSERT INTO t_final_minmax_collapsing VALUES (1, '2026-01-02', -1);

SELECT 'collapsing';
SELECT * FROM t_final_minmax_collapsing FINAL WHERE d = '2026-01-01';

CREATE TABLE t_final_minmax_dnm (k UInt64, d Date, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY toYYYYMM(d) ORDER BY k;
SYSTEM STOP MERGES t_final_minmax_dnm;
INSERT INTO t_final_minmax_dnm VALUES (1, '2026-01-01', 1);
INSERT INTO t_final_minmax_dnm VALUES (1, '2026-01-02', 2);
INSERT INTO t_final_minmax_dnm VALUES (2, '2026-02-01', 1);

SELECT 'do_not_merge_across_partitions_select_final';
SELECT * FROM t_final_minmax_dnm FINAL WHERE d = '2026-01-01' SETTINGS do_not_merge_across_partitions_select_final = 1;
SELECT * FROM t_final_minmax_dnm FINAL WHERE d = '2026-01-02' SETTINGS do_not_merge_across_partitions_select_final = 1;
-- Pruning by partition value still skips the other month.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_final_minmax_dnm FINAL WHERE d = '2026-01-01' SETTINGS do_not_merge_across_partitions_select_final = 1) WHERE explain LIKE '%Parts: 2/3%';

-- Nor by partition value when the partition expression is a float sorting key column: 0.0 and -0.0 are one key in two partitions.
CREATE TABLE t_final_float_partition (k UInt64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY [reinterpretAsFloat64(k)] ORDER BY [reinterpretAsFloat64(k)];
INSERT INTO t_final_float_partition VALUES (0, 1);
INSERT INTO t_final_float_partition VALUES (9223372036854775808, 2);

SELECT 'float sorting key expression';
SELECT * FROM t_final_float_partition FINAL WHERE toString([reinterpretAsFloat64(k)]) = '[0]';
SELECT * FROM t_final_float_partition FINAL WHERE toString([reinterpretAsFloat64(k)]) = '[-0]';

-- Nor when a sorting key column only has the name of the partition expression.
CREATE TABLE t_final_name_collision (k UInt64, d Date, `toYYYYMM(d)` UInt32, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY toYYYYMM(d) ORDER BY (`toYYYYMM(d)`, k);
SYSTEM STOP MERGES t_final_name_collision;
INSERT INTO t_final_name_collision VALUES (1, '2026-01-01', 1, 1);
INSERT INTO t_final_name_collision VALUES (1, '2026-02-01', 1, 2);

SELECT 'column named like the partition expression';
SELECT * FROM t_final_name_collision FINAL WHERE d = '2026-01-01';
SELECT * FROM t_final_name_collision FINAL WHERE d = '2026-02-01';

-- Nor when the sorting key has a lambda of the same signature with another body.
CREATE TABLE t_final_lambda (a Array(UInt64), b UInt64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY arrayMap(x -> x + b, a) ORDER BY arrayMap(x -> x % b, a);
SYSTEM STOP MERGES t_final_lambda;
INSERT INTO t_final_lambda VALUES ([1], 2, 1);
INSERT INTO t_final_lambda VALUES ([3], 2, 2);

SELECT 'lambda with another body';
SELECT * FROM t_final_lambda FINAL WHERE arrayMap(x -> x + b, a) = [3];
SELECT * FROM t_final_lambda FINAL WHERE arrayMap(x -> x + b, a) = [5];

DROP TABLE t_final_minmax_replacing;
DROP TABLE t_final_minmax_collapsing;
DROP TABLE t_final_minmax_dnm;
DROP TABLE t_final_float_partition;
DROP TABLE t_final_name_collision;
DROP TABLE t_final_lambda;
