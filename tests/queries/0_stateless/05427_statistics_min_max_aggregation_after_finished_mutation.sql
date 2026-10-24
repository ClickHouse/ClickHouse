-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- The answer of min/max/count from column statistics checks pending alter and metadata mutations per
-- part. The snapshot also keeps finished mutations for `system.mutations`, and they used to disable
-- the optimization until enough newer mutations evicted them.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_statistics_for_min_max_aggregation = 1;
SET enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS t_05427;
CREATE TABLE t_05427 (x UInt64 STATISTICS(basic), b UInt64 STATISTICS(basic), spare UInt64)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_05427 SELECT number, number + 100, number FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;

SELECT 'before the drop';
SELECT min(x), max(x), count() FROM t_05427;
SELECT count() FROM (EXPLAIN SELECT min(x), max(x) FROM t_05427) WHERE explain LIKE '%_statistics_min_max_projection%';

ALTER TABLE t_05427 DROP COLUMN spare SETTINGS mutations_sync = 2;

SELECT 'after a finished drop';
SELECT min(x), max(x), count() FROM t_05427;
SELECT count() FROM (EXPLAIN SELECT min(x), max(x) FROM t_05427) WHERE explain LIKE '%_statistics_min_max_projection%';

-- A pending `DROP COLUMN b` followed by `ADD COLUMN b` must read the new default, not the stale
-- statistics of the dropped column.
SYSTEM STOP MERGES t_05427;
ALTER TABLE t_05427 DROP COLUMN b SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_05427 ADD COLUMN b UInt64 DEFAULT 7 STATISTICS(basic) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'pending drop and add';
SELECT count() > 0 FROM system.mutations WHERE database = currentDatabase() AND table = 't_05427' AND NOT is_done;
SELECT min(b), max(b), count() FROM t_05427;
SELECT count() FROM (EXPLAIN SELECT min(b), max(b) FROM t_05427) WHERE explain LIKE '%_statistics_min_max_projection%';

DROP TABLE t_05427;
