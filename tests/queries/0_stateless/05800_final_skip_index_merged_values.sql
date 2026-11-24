-- FINAL of engines that build values from several rows must not prune by an index over such a value.
DROP TABLE IF EXISTS t_sum;
CREATE TABLE t_sum (k UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1) ENGINE = SummingMergeTree(v) ORDER BY k;
SYSTEM STOP MERGES t_sum;
INSERT INTO t_sum VALUES (1, 1);
INSERT INTO t_sum VALUES (1, 1);
SELECT k, v FROM t_sum FINAL WHERE v = 2;
DROP TABLE t_sum;

DROP TABLE IF EXISTS t_agg;
CREATE TABLE t_agg (k UInt64, v SimpleAggregateFunction(sum, UInt64), INDEX idx_v v TYPE minmax GRANULARITY 1) ENGINE = AggregatingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_agg;
INSERT INTO t_agg VALUES (1, 1);
INSERT INTO t_agg VALUES (1, 1);
SELECT k, v FROM t_agg FINAL WHERE v = 2;
DROP TABLE t_agg;

DROP TABLE IF EXISTS t_coal;
CREATE TABLE t_coal (k UInt64, a Nullable(UInt64), b Nullable(UInt64), INDEX idx_a a TYPE minmax GRANULARITY 1) ENGINE = CoalescingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_coal;
INSERT INTO t_coal VALUES (1, 1, NULL);
INSERT INTO t_coal VALUES (1, NULL, 2);
SELECT k, a, b FROM t_coal FINAL WHERE b = 2 AND a = 1;
DROP TABLE t_coal;
