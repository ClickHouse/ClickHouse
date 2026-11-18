-- Tags: shard

-- A join after an `ARRAY JOIN` of the leftmost table must pass the same parallel replicas check as
-- without the `ARRAY JOIN`. A `RIGHT JOIN` whose right side cannot be read with parallel replicas, and a
-- `CROSS JOIN`, returned every right row (every row for `CROSS`) once per replica.
-- https://github.com/ClickHouse/ClickHouse/issues/130345, https://github.com/ClickHouse/ClickHouse/issues/120961

SET automatic_parallel_replicas_mode = 0, parallel_replicas_plan_based = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;
-- With the local plan, a right side without a `MergeTree` read makes the initiator fall back to a local
-- query, which hides the wrong decision.
SET parallel_replicas_local_plan = 0;
SET explain_query_plan_default = 'legacy';
SET join_algorithm = 'hash', join_use_nulls = 0;

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS mt2;
DROP TABLE IF EXISTS mem;

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE mt2 (y UInt64) ENGINE = MergeTree ORDER BY y;
CREATE TABLE mem (y UInt64) ENGINE = Memory;
INSERT INTO mt SELECT number FROM numbers(10);
INSERT INTO mt2 SELECT number FROM numbers(10);
INSERT INTO mem SELECT number FROM numbers(10);

SELECT 'right join to Memory: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT count() FROM mt ARRAY JOIN [1] AS a RIGHT JOIN mem ON mt.x = mem.y);
SELECT count(), sum(a), sum(mem.y) FROM mt ARRAY JOIN [1] AS a RIGHT JOIN mem ON mt.x = mem.y;

SELECT 'right join to a subquery: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT count() FROM mt ARRAY JOIN [1] AS a RIGHT JOIN (SELECT number AS y FROM numbers(10)) AS s ON mt.x = s.y);
SELECT count(), sum(a), sum(s.y) FROM mt ARRAY JOIN [1] AS a RIGHT JOIN (SELECT number AS y FROM numbers(10)) AS s ON mt.x = s.y;

SELECT 'cross join: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT count() FROM mt ARRAY JOIN [1] AS a CROSS JOIN mt2);
SELECT count(), sum(a), sum(mt2.y) FROM mt ARRAY JOIN [1] AS a CROSS JOIN mt2;

-- A `MergeTree` table on the right is read with parallel replicas, as without the `ARRAY JOIN`.
SELECT 'right join to MergeTree: reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') FROM (
    EXPLAIN SELECT count() FROM mt ARRAY JOIN [1] AS a RIGHT JOIN mt2 ON mt.x = mt2.y);
SELECT count(), sum(a), sum(mt2.y) FROM mt ARRAY JOIN [1] AS a RIGHT JOIN mt2 ON mt.x = mt2.y;

DROP TABLE mt;
DROP TABLE mt2;
DROP TABLE mem;
