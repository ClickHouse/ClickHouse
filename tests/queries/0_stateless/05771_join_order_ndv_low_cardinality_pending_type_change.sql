-- The number of distinct values of a `LowCardinality` column is not taken from the dictionary of a part that still has the
-- previous type of the column, before the mutation of `ALTER MODIFY COLUMN` rewrites it: the dictionary of another type is
-- not read as one of the new type.

SET use_statistics = 1;
SET estimate_distinct_values_from_metadata = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET mutations_sync = 0;
SET alter_sync = 0;

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, s LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0;

INSERT INTO t VALUES (1, 'ab'), (2, 'cd');

SYSTEM STOP MERGES t;
ALTER TABLE t MODIFY COLUMN s LowCardinality(FixedString(3));

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't' AND NOT is_done;

SELECT count() FROM t AS a JOIN t AS b ON a.s = b.s;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.s = b.s);

SYSTEM START MERGES t;
DROP TABLE t;
