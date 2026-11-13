-- FINAL of these engines can return a row that matches the filter while none of the merged rows does

SET use_skip_indexes = 1, use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1;

DROP TABLE IF EXISTS t_summing;
DROP TABLE IF EXISTS t_aggregating;
DROP TABLE IF EXISTS t_aggregating_plain;
DROP TABLE IF EXISTS t_coalescing;

CREATE TABLE t_summing (k UInt64, v UInt64, INDEX idx_k k TYPE minmax GRANULARITY 1, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = SummingMergeTree(v) ORDER BY k;
SYSTEM STOP MERGES t_summing;
INSERT INTO t_summing VALUES (1, 1);
INSERT INTO t_summing VALUES (1, 1);

SELECT 'summing', * FROM t_summing FINAL WHERE v = 2;
SELECT * FROM t_summing FINAL WHERE v = 2 SETTINGS force_data_skipping_indices = 'idx_v'; -- { serverError INDEX_NOT_USED }
SELECT 'summing key', * FROM t_summing FINAL WHERE k = 1 SETTINGS force_data_skipping_indices = 'idx_k';
SELECT * FROM t_summing FINAL WHERE v = 2 FORMAT Null SETTINGS use_skip_indexes_if_final_exact_mode = 0, force_data_skipping_indices = 'idx_v';

CREATE TABLE t_aggregating (k UInt64, a SimpleAggregateFunction(max, UInt64), b SimpleAggregateFunction(min, UInt64),
    INDEX idx_a a TYPE minmax GRANULARITY 1, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = AggregatingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_aggregating;
INSERT INTO t_aggregating VALUES (1, 5, 9);
INSERT INTO t_aggregating VALUES (1, 3, 7);

SELECT 'aggregating', * FROM t_aggregating FINAL WHERE a = 5 AND b = 7;

CREATE TABLE t_aggregating_plain (k UInt64, p UInt64, INDEX idx_p p TYPE minmax GRANULARITY 1)
ENGINE = AggregatingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_aggregating_plain;
INSERT INTO t_aggregating_plain VALUES (1, 5);
INSERT INTO t_aggregating_plain VALUES (2, 3);

SELECT 'aggregating plain', * FROM t_aggregating_plain FINAL WHERE p = 5 SETTINGS force_data_skipping_indices = 'idx_p';

CREATE TABLE t_coalescing (k UInt64, a Nullable(UInt64), b Nullable(UInt64),
    INDEX idx_a a TYPE minmax GRANULARITY 1, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = CoalescingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_coalescing;
INSERT INTO t_coalescing VALUES (1, 5, NULL);
INSERT INTO t_coalescing VALUES (1, NULL, 7);

SELECT 'coalescing', * FROM t_coalescing FINAL WHERE a = 5 AND b = 7;

DROP TABLE t_summing;
DROP TABLE t_aggregating;
DROP TABLE t_aggregating_plain;
DROP TABLE t_coalescing;
