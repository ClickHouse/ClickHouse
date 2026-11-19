-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert
-- no-fasttest: a UNIQUE KEY insert writes the dense-index SST, which needs RocksDB.
-- no-object-storage, no-s3-storage: UNIQUE KEY requires a storage policy of local disks.
-- no-ordinary-database, no-replicated-database, no-shared-merge-tree: UNIQUE KEY is only supported on plain MergeTree
--                       in an Atomic database.
-- no-async-insert: the superseding row must be its own part, inserted after the first one.

-- A UNIQUE KEY read drops the rows superseded by later inserts through the delete bitmaps of its snapshot. They are not
-- lightweight deletes and leave no mutation, but the part statistics still cover them. So once a part has a superseded
-- row, its statistics no longer describe the read: IEJoin does not pick its key conditions by their min/max but falls
-- back to syntax order.

SET enable_unique_key = 1;
SET join_algorithm = 'ie_join';
SET join_use_nulls = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The printed conditions are mirrored when the join order optimizer swaps the sides
-- (e.g. under randomized `query_plan_optimize_join_order_randomize`); disable it to keep them stable.
SET query_plan_optimize_join_order_limit = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS t_relation_stats_uk_l;
DROP TABLE IF EXISTS t_relation_stats_uk_r;

CREATE TABLE t_relation_stats_uk_l (a1 UInt32, a2 UInt32, a3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_relation_stats_uk_r (id UInt32, b1 UInt32, b2 UInt32, b3 UInt32)
ENGINE = MergeTree ORDER BY id UNIQUE KEY id
SETTINGS auto_statistics_types = 'basic, uniq_v2', unique_key_conflict_action = 'overwrite';

-- A merge would drop the superseded row, and its bitmap with it.
SYSTEM STOP MERGES t_relation_stats_uk_r;

-- As in 05023: sel(a1 < b1) ~ 0.5, sel(a2 < b2) = 1 and sel(a3 < b3) ~ 0.005, so the statistics pick
-- `a1 < b1 AND a3 < b3`, while the first two in syntax order are `a1 < b1 AND a2 < b2`.
INSERT INTO t_relation_stats_uk_l SELECT number % 1000, number % 1000, (number * 97) % 100000 FROM numbers(1000);
INSERT INTO t_relation_stats_uk_r SELECT number, number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);

SELECT '-- every row is live: the statistics describe the read and pick the keys';
SELECT count(), min(b1) FROM t_relation_stats_uk_r;

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_relation_stats_uk_l AS l JOIN t_relation_stats_uk_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- Supersedes id 0, the only row with b1 = 0. Its granule keeps 999 live rows, so it is not dropped as fully dead, and
-- the statistics of the first part still hold b1 = 0.
INSERT INTO t_relation_stats_uk_r VALUES (0, 500, 1500, 500);

SELECT '-- id 0 superseded: the statistics still cover its row, so the keys are in syntax order';
SELECT count(), min(b1) FROM t_relation_stats_uk_r;

SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_relation_stats_uk_l AS l JOIN t_relation_stats_uk_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- The join reads only the live rows; the same predicate as a filter over a CROSS JOIN is the oracle.
SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_relation_stats_uk_l AS l JOIN t_relation_stats_uk_r AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a2 + a3 + b1 + b2 + b3) FROM t_relation_stats_uk_l AS l, t_relation_stats_uk_r AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

DROP TABLE t_relation_stats_uk_l;
DROP TABLE t_relation_stats_uk_r;
