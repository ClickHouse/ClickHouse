-- A read through `mergeTreeProjection` under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- The read is serialized as a read of the parent table, so a replica that got it would read the parent's own parts
-- instead of the projection parts: the rows of the base table, or `NOT_FOUND_COLUMN_IN_BLOCK` for a column only the
-- projection has. Such a read stays on the initiator. Without the local plan every replica is a remote one, so a
-- shipped read would always reach one.

DROP TABLE IF EXISTS t_pr_projection_read;
DROP TABLE IF EXISTS t_pr_projection_offset;
DROP VIEW IF EXISTS v_pr_projection_read;

-- The projection keeps a tenth of the rows.
CREATE TABLE t_pr_projection_read (a UInt32, b UInt32, PROJECTION p (SELECT a, b WHERE a % 10 = 0 ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;
INSERT INTO t_pr_projection_read SELECT number, number % 7 FROM numbers(100000);

-- `_parent_part_offset` exists only in the projection.
CREATE TABLE t_pr_projection_offset (a Int32, b Int32, PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;
INSERT INTO t_pr_projection_offset SELECT number, number % 100 FROM numbers(30000);

-- A view hides the table function from the query, but not from its plan.
CREATE VIEW v_pr_projection_read AS SELECT a, b FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p);

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 0;

SELECT '--- the rows of the projection, not of the table';
SELECT count(), sum(a), countIf(a % 10 != 0) FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p);

SELECT '--- a column only the projection has';
SELECT count(), uniqExact(_parent_part_offset) FROM mergeTreeProjection(currentDatabase(), t_pr_projection_offset, p);
SELECT sum(l._part_offset = r._parent_part_offset)
FROM t_pr_projection_offset AS l JOIN mergeTreeProjection(currentDatabase(), t_pr_projection_offset, p) AS r USING (a);

SELECT '--- an IN set built from the projection';
-- Every replica builds the set by running its plan, so a read through `mergeTreeProjection` there keeps the fragment local too.
SELECT count() FROM t_pr_projection_read WHERE a IN (SELECT a FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p));
SELECT count() FROM t_pr_projection_read WHERE a NOT IN (SELECT a FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p));
SELECT count() FROM t_pr_projection_offset WHERE _part_offset IN (SELECT _parent_part_offset FROM mergeTreeProjection(currentDatabase(), t_pr_projection_offset, p));

SELECT '--- a nested IN set built from the projection';
SELECT count() FROM t_pr_projection_read
WHERE a IN (SELECT a FROM t_pr_projection_read WHERE a IN (SELECT a FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p)));
SELECT sum(a IN (SELECT a FROM t_pr_projection_read WHERE a IN (SELECT a FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p))))
FROM t_pr_projection_read;

SELECT '--- a nested IN set in LIMIT AFTER, LIMIT UNTIL and INTERPOLATE';
-- Eager index analysis would build the sets before the check, so it is turned off.
SELECT sum(a) FROM t_pr_projection_read
WHERE a IN (SELECT a FROM t_pr_projection_read ORDER BY a
            LIMIT 10 AFTER a > 5 AND a IN (SELECT a FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p)))
SETTINGS use_index_for_in_with_subqueries = 0;
SELECT sum(a) FROM t_pr_projection_read
WHERE a IN (SELECT a FROM t_pr_projection_read ORDER BY a
            LIMIT UNTIL a > 5 AND a IN (SELECT a FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p)))
SETTINGS use_index_for_in_with_subqueries = 0;
SELECT sum(a) FROM t_pr_projection_read
WHERE a IN (SELECT v FROM (SELECT a, a AS v FROM t_pr_projection_read WHERE a % 10 = 5 AND a < 100)
            ORDER BY a WITH FILL
            INTERPOLATE (v AS v + 1000 * (a IN (SELECT a FROM mergeTreeProjection(currentDatabase(), t_pr_projection_read, p)))))
SETTINGS use_index_for_in_with_subqueries = 0;

SELECT '--- a view over the projection';
SELECT count(), sum(a) FROM v_pr_projection_read;
SELECT count() FROM t_pr_projection_read WHERE a IN (SELECT a FROM v_pr_projection_read);
SELECT count() FROM t_pr_projection_read WHERE a IN (SELECT a FROM t_pr_projection_read WHERE a IN (SELECT a FROM v_pr_projection_read));
SELECT count() FROM t_pr_projection_read AS l JOIN v_pr_projection_read AS r USING (a);

DROP VIEW v_pr_projection_read;
DROP TABLE t_pr_projection_read;
DROP TABLE t_pr_projection_offset;
