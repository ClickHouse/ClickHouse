-- EXPLAIN WHATIF uses a hypothetical object as CREATE accepted it; the object is not applicable only when a column or setting that it binds to changed

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_bind;
CREATE TABLE t_bind (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_bind SELECT number, number FROM numbers(10000);

SELECT 'a setting of the EXPLAIN query does not change what CREATE accepted';
SET allow_suspicious_indexes = 1;
CREATE HYPOTHETICAL INDEX i_dup ON t_bind (b, b) TYPE minmax;
SET allow_suspicious_indexes = 0;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_bind WHERE b = 42) WHERE explain LIKE '  status:%';
DROP HYPOTHETICAL INDEX i_dup ON t_bind;

SELECT 'a new column that the index does not read keeps the index';
CREATE HYPOTHETICAL INDEX i_b ON t_bind (b) TYPE minmax;
ALTER TABLE t_bind ADD COLUMN c UInt64;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_bind WHERE b = 42) WHERE explain LIKE '  status:%';

SELECT 'a type change of a column that the index reads makes the index not applicable';
ALTER TABLE t_bind MODIFY COLUMN b UInt32 SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_bind WHERE b = 42) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
DROP HYPOTHETICAL INDEX i_b ON t_bind;

SELECT 'a new column under SELECT * makes the projection not applicable';
CREATE HYPOTHETICAL PROJECTION p_all ON t_bind (SELECT * ORDER BY b);
ALTER TABLE t_bind ADD COLUMN d UInt64;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_bind WHERE b = 42) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';

DROP TABLE t_bind;
