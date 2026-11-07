SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_convert_join_to_in = 1;
SET join_algorithm = 'hash';
SET use_index_for_in_with_subqueries = 1;
SET optimize_move_to_prewhere = 0;
SET prefer_localhost_replica = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

CREATE TABLE mutation_merge_keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mutation_merge_keys VALUES (0), (1), (2);
CREATE TABLE mutation_merge_rows (k UInt64) ENGINE = Memory;
INSERT INTO mutation_merge_rows VALUES (0), (1), (2);
CREATE TABLE mutation_merge_target (k UInt64, v UInt8) ENGINE = Memory;
INSERT INTO mutation_merge_target VALUES (0, 0), (1, 0), (2, 0);
CREATE TABLE mutation_merge_dist AS mutation_merge_rows
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), mutation_merge_rows);
CREATE VIEW mutation_merge_view AS SELECT toUInt64(1) AS k;

-- The filter is pushed into the `Merge` child plans, which rely on the outer CTE gate.
-- `JOIN` -> `IN` must not extract them into a set built synchronously by index analysis.
ALTER TABLE mutation_merge_target UPDATE v = k IN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT a.k FROM mutation_merge_keys AS a ANY INNER JOIN
    (
        SELECT k FROM merge(currentDatabase(), '^mutation_merge_(view|dist)$')
        WHERE k IN (SELECT k FROM c UNION ALL SELECT k FROM c)
    ) AS b ON a.k = b.k
)
WHERE 1;
SELECT k, v FROM mutation_merge_target ORDER BY k;

-- Compare with the same mutation without the conversion.
SET query_plan_convert_join_to_in = 0;
ALTER TABLE mutation_merge_target UPDATE v = 0 WHERE 1;
ALTER TABLE mutation_merge_target UPDATE v = k IN
(
    WITH c AS MATERIALIZED (SELECT number AS k FROM numbers(2))
    SELECT a.k FROM mutation_merge_keys AS a ANY INNER JOIN
    (
        SELECT k FROM merge(currentDatabase(), '^mutation_merge_(view|dist)$')
        WHERE k IN (SELECT k FROM c UNION ALL SELECT k FROM c)
    ) AS b ON a.k = b.k
)
WHERE 1;
SELECT k, v FROM mutation_merge_target ORDER BY k;

DROP VIEW mutation_merge_view;
DROP TABLE mutation_merge_dist;
DROP TABLE mutation_merge_target;
DROP TABLE mutation_merge_rows;
DROP TABLE mutation_merge_keys;
