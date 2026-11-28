-- Tags: no-parallel-replicas

-- Two normal projections, one of them materialized only in some parts. The analysis of a candidate
-- removes from the parent read the parts in which the candidate selects nothing, and a candidate
-- analyzed earlier counted those parts into its cost when it is not materialized on them. The costs
-- must be recomputed from the surviving parent parts before the candidates are ranked, otherwise
-- the choice depends on the declaration order of the projections. Skip indexes play no role here.

SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 0;
SET use_skip_indexes = 0;
SET mutations_sync = 2;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab_p1_first;
DROP TABLE IF EXISTS tab_p2_first;

CREATE TABLE tab_p1_first
(
    p UInt8,
    a UInt32,
    b UInt32,
    PROJECTION p1 (SELECT * ORDER BY b),
    PROJECTION p2 (SELECT * ORDER BY a)
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple()
SETTINGS index_granularity = 64, index_granularity_bytes = 0, use_const_adaptive_granularity = 1, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

CREATE TABLE tab_p2_first
(
    p UInt8,
    a UInt32,
    b UInt32,
    PROJECTION p2 (SELECT * ORDER BY a),
    PROJECTION p1 (SELECT * ORDER BY b)
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY tuple()
SETTINGS index_granularity = 64, index_granularity_bytes = 0, use_const_adaptive_granularity = 1, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- Partition 0: 100 granules, all with a = 0. Partition 1: 1000 granules, all with a = 2.
INSERT INTO tab_p1_first SELECT 0, 0, number FROM numbers(6400);
INSERT INTO tab_p1_first SELECT 1, 2, number FROM numbers(64000);
INSERT INTO tab_p2_first SELECT 0, 0, number FROM numbers(6400);
INSERT INTO tab_p2_first SELECT 1, 2, number FROM numbers(64000);

-- `p1` stays materialized only in partition 0, so partition 1 is read from the parent when `p1` is chosen.
ALTER TABLE tab_p1_first CLEAR PROJECTION p1 IN PARTITION 1;
ALTER TABLE tab_p2_first CLEAR PROJECTION p1 IN PARTITION 1;

-- For `a = 0 AND b = 1`: `p1` selects one granule of partition 0 plus the 1000 parent granules of
-- partition 1 until the analysis of `p2` proves partition 1 empty (`a = 2` there) and removes it from
-- the parent read. `p2` selects all 100 granules of partition 0, which is no better than the parent.
-- `p1` must win in both declaration orders.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_p1_first WHERE a = 0 AND b = 1) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() FROM tab_p1_first WHERE a = 0 AND b = 1;

SELECT trimLeft(explain) FROM (EXPLAIN SELECT count() FROM tab_p2_first WHERE a = 0 AND b = 1) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() FROM tab_p2_first WHERE a = 0 AND b = 1;

DROP TABLE tab_p1_first;
DROP TABLE tab_p2_first;
