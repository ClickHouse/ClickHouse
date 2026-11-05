-- a real projection added later under the name of a hypothetical one makes the hypothetical one not applicable

SET enable_parallel_replicas = 0, optimize_use_projections = 1;

DROP TABLE IF EXISTS t_name;
CREATE TABLE t_name (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_name SELECT number, number % 5 FROM numbers(1000);

CREATE HYPOTHETICAL PROJECTION p ON t_name (SELECT a, b ORDER BY b);
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_name WHERE b = 1) WHERE explain LIKE '  status:%';

ALTER TABLE t_name ADD PROJECTION p (SELECT a, b ORDER BY a);
ALTER TABLE t_name MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_name WHERE b = 1) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';

DROP TABLE t_name;
