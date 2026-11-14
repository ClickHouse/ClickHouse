-- a column that only an `APPLY` lambda reads is bound too, so its type change or removal makes the hypothetical projection not applicable

SET enable_parallel_replicas = 0, optimize_use_projections = 1;

DROP TABLE IF EXISTS t_apply;
CREATE TABLE t_apply (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_apply SELECT number, 1 FROM numbers(1000);

CREATE HYPOTHETICAL PROJECTION p ON t_apply (SELECT a, count(), COLUMNS(a) APPLY (x -> sum(x + b)) GROUP BY a);
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, count() FROM t_apply WHERE a = 42 GROUP BY a) WHERE explain LIKE '  status:%';

ALTER TABLE t_apply MODIFY COLUMN b Float64 SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, count() FROM t_apply WHERE a = 42 GROUP BY a) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';

ALTER TABLE t_apply DROP COLUMN b SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, count() FROM t_apply WHERE a = 42 GROUP BY a) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';

DROP TABLE t_apply;
