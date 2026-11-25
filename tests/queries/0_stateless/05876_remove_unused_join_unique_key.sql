-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert
-- no-fasttest: a UNIQUE KEY insert writes the dense-index SST, which needs RocksDB.
-- no-object-storage, no-s3-storage: UNIQUE KEY requires a storage policy of local disks.
-- no-ordinary-database, no-replicated-database, no-shared-merge-tree: UNIQUE KEY is only supported on plain MergeTree
--                       in an Atomic database.
-- no-async-insert: the second INSERT into `t2` must stay its own part (a coalesced batch repeating a key is rejected).

-- A `LEFT` or `RIGHT` join is removed when its other side is not read and is a table without a partition key
-- read by its whole `UNIQUE KEY`, which outputs every row of the preserved side once.

SET enable_unique_key = 1;
SET query_plan_remove_unused_joins = 1;
SET query_plan_remove_unused_columns = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0; -- the joins of the plan are counted
SET join_use_nulls = 0; -- the filter of the `ANY` join must reject the default of a not-matched row

DROP VIEW IF EXISTS v_left;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t_ab;
DROP TABLE IF EXISTS t_part;

CREATE TABLE t1 (id UInt64, v1 UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, v2 UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY id;
CREATE TABLE t3 (id UInt64, v3 UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY id;
CREATE TABLE t_ab (a UInt64, b UInt64, name String) ENGINE = MergeTree ORDER BY (a, b) UNIQUE KEY (a, b);
CREATE TABLE t_part (p UInt8, id UInt64) ENGINE = MergeTree ORDER BY id UNIQUE KEY id PARTITION BY p;
CREATE VIEW v_left AS SELECT id, v1, v2, v3 FROM t1 LEFT JOIN t2 USING (id) LEFT JOIN t3 USING (id);

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (1, 100), (2, 200);
INSERT INTO t2 VALUES (1, 101); -- supersedes the row of id 1: the parts hold two rows of it, a read outputs one
INSERT INTO t3 VALUES (1, 1000), (3, 3000);
INSERT INTO t_ab VALUES (1, 10, 'x'), (1, 11, 'y');
INSERT INTO t_part VALUES (1, 1), (2, 1); -- id 1 once in each of two partitions

-- { echoOn }

-- A view that joins tables sharing a key.
SELECT count() FROM (EXPLAIN SELECT sum(v1) FROM v_left) WHERE explain LIKE '%Join (%';
SELECT count(), sum(v1) FROM v_left;
SELECT count(), sum(v1) FROM v_left SETTINGS query_plan_remove_unused_joins = 0;

-- Only the join whose other side is not read goes away.
SELECT count() FROM (EXPLAIN SELECT sum(v2) FROM v_left) WHERE explain LIKE '%Join (%';
SELECT sum(v2) FROM v_left;

-- `RIGHT`.
SELECT count() FROM (EXPLAIN SELECT t1.id FROM t2 RIGHT JOIN t1 ON t2.id = t1.id) WHERE explain LIKE '%Join (%';
SELECT t1.id FROM t2 RIGHT JOIN t1 ON t2.id = t1.id ORDER BY t1.id;

-- The join keys must cover the whole `UNIQUE KEY`.
SELECT count() FROM (EXPLAIN SELECT id FROM t1 LEFT JOIN t_ab ON t1.id = t_ab.a) WHERE explain LIKE '%Join (%';
SELECT id FROM t1 LEFT JOIN t_ab ON t1.id = t_ab.a ORDER BY id;
SELECT count() FROM (EXPLAIN SELECT id FROM t1 LEFT JOIN t_ab ON t1.id = t_ab.a AND t1.v1 = t_ab.b) WHERE explain LIKE '%Join (%';
SELECT id FROM t1 LEFT JOIN t_ab ON t1.id = t_ab.a AND t1.v1 = t_ab.b ORDER BY id;

-- A table with a partition key is unique by its `UNIQUE KEY` within each partition only.
SELECT count() FROM (EXPLAIN SELECT id FROM t1 LEFT JOIN t_part ON t1.id = t_part.id) WHERE explain LIKE '%Join (%';
SELECT id FROM t1 LEFT JOIN t_part ON t1.id = t_part.id ORDER BY id;

-- An outer `ANY` join with a filter that rejects the rows without a match becomes `INNER`.
SELECT count() FROM (EXPLAIN actions = 1, keep_logical_steps = 1 SELECT id, v2 FROM t1 LEFT ANY JOIN t2 USING (id) WHERE v2 > 0) WHERE explain LIKE '%Type: INNER%';
SELECT id, v2 FROM t1 LEFT ANY JOIN t2 USING (id) WHERE v2 > 0 ORDER BY id;

-- { echoOff }

DROP VIEW v_left;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t_ab;
DROP TABLE t_part;
