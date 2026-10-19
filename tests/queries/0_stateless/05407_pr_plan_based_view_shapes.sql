-- The plan-based counterpart of the `-- what is shipped to replicas` section of
-- `04728_pr_view_internal_settings` and of the `EXPLAIN` assertions of
-- `04401_parallel_replicas_view_over_mergetree_per_query_setting`. Both read the relation out of the
-- SQL the initiator ships, which only the query-based implementation produces, so they are pinned to
-- it; what survives the implementation is which of these views the reads are distributed for.
--
-- That is the property those tests are really about: a `SETTINGS` clause in a view body that opts out
-- of parallel replicas has to be honoured, or every replica reads the whole table and the query
-- returns `max_parallel_replicas` times too many rows. `index_granularity = 1` keeps that failure
-- deterministic, as it does there.

DROP VIEW IF EXISTS v_plain;
DROP VIEW IF EXISTS v_set;
DROP VIEW IF EXISTS v_nested;
DROP VIEW IF EXISTS v_union;
DROP VIEW IF EXISTS v_union_both;
DROP TABLE IF EXISTS mt_g;
DROP TABLE IF EXISTS mt_g2;

CREATE TABLE mt_g (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE mt_g2 (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO mt_g SELECT number FROM numbers(300);
INSERT INTO mt_g2 SELECT number FROM numbers(300);

CREATE VIEW v_plain AS SELECT a FROM mt_g;
CREATE VIEW v_set AS SELECT a FROM mt_g SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;
CREATE VIEW v_nested AS SELECT a FROM v_set;
CREATE VIEW v_union AS
    SELECT a FROM mt_g SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0
    UNION ALL
    SELECT a FROM mt_g2;
CREATE VIEW v_union_both AS
    SELECT a FROM mt_g SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0
    UNION ALL
    SELECT a FROM mt_g2 SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    parallel_replicas_plan_based = 1;

-- Per view: the rows it returns, and whether the read was distributed. A body that opts out is not
-- distributed and still returns its own rows; a union distributes the branch that did not opt out.
SELECT 'v_plain', count() FROM v_plain;
SELECT 'v_plain distributed', countIf(explain ILIKE '%ParallelReplicas%') FROM (EXPLAIN SELECT count() FROM v_plain);

SELECT 'v_set', count() FROM v_set;
SELECT 'v_set distributed', countIf(explain ILIKE '%ParallelReplicas%') FROM (EXPLAIN SELECT count() FROM v_set);

SELECT 'v_nested', count() FROM v_nested;
SELECT 'v_nested distributed', countIf(explain ILIKE '%ParallelReplicas%') FROM (EXPLAIN SELECT count() FROM v_nested);

SELECT 'v_union', count() FROM v_union;
SELECT 'v_union distributed', countIf(explain ILIKE '%ParallelReplicas%') FROM (EXPLAIN SELECT count() FROM v_union);

SELECT 'v_union_both', count() FROM v_union_both;
SELECT 'v_union_both distributed', countIf(explain ILIKE '%ParallelReplicas%') FROM (EXPLAIN SELECT count() FROM v_union_both);

-- `parallel_replicas_allow_view_over_mergetree` is read in `findParallelReplicasQuery` and
-- `PlannerJoinTree`, which only the query-based decision goes through, so it does not gate this path
-- at either value. `04401_parallel_replicas_view_over_mergetree_per_query_setting` covers what the
-- setting does where it is honoured.
SELECT 'gate off, distributed', countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT count() FROM v_plain SETTINGS parallel_replicas_allow_view_over_mergetree = 0);
SELECT 'gate on, distributed', countIf(explain ILIKE '%ParallelReplicas%') FROM (
    EXPLAIN SELECT count() FROM v_plain SETTINGS parallel_replicas_allow_view_over_mergetree = 1);

DROP VIEW v_union_both;
DROP VIEW v_union;
DROP VIEW v_nested;
DROP VIEW v_set;
DROP VIEW v_plain;
DROP TABLE mt_g2;
DROP TABLE mt_g;
