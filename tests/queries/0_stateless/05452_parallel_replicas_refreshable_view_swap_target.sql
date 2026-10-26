-- Tags: no-replicated-database
-- Tag no-replicated-database: a Replicated database refuses a non-APPEND refreshable materialized view over a plain MergeTree table.

-- A non-APPEND refreshable materialized view replaces its target on every refresh, so the target is read locally rather than
-- with parallel replicas. The target of an APPEND view is not replaced and is still read with parallel replicas.

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_plan_based = 0,
    automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number FROM numbers(1000);

CREATE TABLE dest_replace (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW rmv_replace REFRESH EVERY 1 YEAR TO dest_replace AS SELECT x FROM src;
SYSTEM WAIT VIEW rmv_replace;

CREATE TABLE dest_append (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW rmv_append REFRESH EVERY 1 YEAR APPEND TO dest_append AS SELECT x FROM src;
SYSTEM WAIT VIEW rmv_append;

-- A: direct read of the replaced target.
SELECT sum(x) FROM dest_replace;
SELECT count() FROM (EXPLAIN SELECT sum(x) FROM dest_replace) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

-- B: the target inside a subquery that would be sent whole to the replicas.
SELECT count() FROM (EXPLAIN SELECT sum(x) FROM (SELECT x FROM dest_replace WHERE x > 10)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
-- The APPEND target in the same shape is still read with parallel replicas.
SELECT count() > 0 FROM (EXPLAIN SELECT sum(x) FROM (SELECT x FROM dest_append WHERE x > 10)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

-- C: the APPEND target is still read with parallel replicas.
SELECT sum(x) FROM dest_append;
SELECT count() > 0 FROM (EXPLAIN SELECT sum(x) FROM dest_append) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

-- D: the target as the right side of a RIGHT JOIN, the side the replicas read with parallel replicas.
SELECT sum(x) FROM src RIGHT JOIN dest_replace USING (x);
SELECT count() FROM (EXPLAIN SELECT sum(x) FROM src RIGHT JOIN dest_replace USING (x)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
-- The APPEND target in the same shape is still read with parallel replicas.
SELECT count() > 0 FROM (EXPLAIN SELECT sum(x) FROM src RIGHT JOIN dest_append USING (x)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

-- E: the target behind an Alias table or an ordinary materialized view, which forward the read to it.
CREATE TABLE alias_replace ENGINE = Alias('dest_replace');
CREATE TABLE alias_append ENGINE = Alias('dest_append');
CREATE MATERIALIZED VIEW mv_replace TO dest_replace AS SELECT x FROM src;
CREATE MATERIALIZED VIEW mv_append TO dest_append AS SELECT x FROM src;
SELECT count() FROM (EXPLAIN SELECT sum(x) FROM alias_replace) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM (EXPLAIN SELECT sum(x) FROM mv_replace) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
-- The same wrappers over the APPEND target are still read with parallel replicas.
SELECT count() > 0 FROM (EXPLAIN SELECT sum(x) FROM alias_append) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() > 0 FROM (EXPLAIN SELECT sum(x) FROM mv_append) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

DROP TABLE mv_replace;
DROP TABLE mv_append;
DROP TABLE alias_replace;
DROP TABLE alias_append;
DROP TABLE rmv_replace;
DROP TABLE rmv_append;
DROP TABLE dest_replace;
DROP TABLE dest_append;
DROP TABLE src;
