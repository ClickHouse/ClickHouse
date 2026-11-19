-- DPsub's interesting-order costing reads the key range of a sorted probe table and of its build side to estimate how much
-- of the probe the runtime filter of a hash join lets through, or a sharded merge join reads. The two roles need different
-- guarantees of a range: the keys of the build side must lie in its range, and the rows of the probe must be spread over
-- its range, which a range of a column that rows were dropped from by a predicate on the column is not. A range that does
-- not give the guarantee of its role is not used: the probe is then charged in full, and the build side does not bound it.
-- The statistics hints below give the sizes and ranges the plans are chosen for. Hints stand in for measured statistics,
-- so the read of a hinted table records what it does to the rows as with measured statistics: a predicate, as a filter or
-- as PREWHERE, counts as dropping rows only of the columns it reads; a row drop that is not attributed to columns, such as
-- one by a read limit, counts for every column; and `FINAL` may rewrite values. The tables with measured statistics cover
-- what hints cannot describe.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET enable_join_transitive_predicates = 1;
SET optimize_read_in_order = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET query_plan_join_swap_table = 'auto';
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET optimize_sorting_by_input_stream_properties = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
-- The runtime filter skips probe granules at read time only with this on.
SET use_skip_indexes_on_data_read = 1;
-- The hinted tables have no statistics, so they are estimated from the hints.
SET use_statistics = 1;
SET use_hash_table_stats_for_join_reordering = 0;
-- A merge join of sorted inputs is costed as sharded by primary key ranges only with this on.
SET query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build_narrow;
DROP TABLE IF EXISTS build_wide;
DROP TABLE IF EXISTS build_tenth_unsorted;
DROP TABLE IF EXISTS build_tenth_final;
DROP TABLE IF EXISTS build_measured;
DROP TABLE IF EXISTS build_partial;

CREATE TABLE probe (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE build_narrow (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE build_wide (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE build_tenth_unsorted (k Int64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE build_tenth_final (k Int64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY v SETTINGS auto_statistics_types = '';
CREATE TABLE build_measured (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE build_partial (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO probe SELECT number, number FROM numbers(1000);
INSERT INTO build_narrow SELECT number, number FROM numbers(100);
INSERT INTO build_wide SELECT number * 10, number FROM numbers(100);
INSERT INTO build_tenth_unsorted SELECT number, number FROM numbers(100);
INSERT INTO build_tenth_final SELECT number, number FROM numbers(100);
INSERT INTO build_measured SELECT number, number FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
-- A part without statistics: the statistics of the table cover only some of its rows. A merge would add them.
SYSTEM STOP MERGES build_partial;
INSERT INTO build_partial SELECT number, number FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO build_partial SELECT number + 1000, number FROM numbers(10) SETTINGS materialize_statistics_on_insert = 0;

-- `build_narrow` covers a hundredth of the probe key range, `build_tenth_unsorted` and `build_tenth_final` a tenth, and
-- `build_wide` all of it. `build_measured` covers 1000 keys of the probe.
SET param__internal_join_table_stat_hints = '{
    "probe": { "cardinality": 200000000, "distinct_keys": { "k": 200000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_narrow": { "cardinality": 2000000, "distinct_keys": { "k": 2000000 }, "key_ranges": { "k": [0, 2000000] } },
    "build_wide": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_tenth_unsorted": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 20000000] } },
    "build_tenth_final": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 20000000] } }
}';

-- Without the runtime filter, only the sharded merge join reads less of the probe when the build keys lie in a narrow range.
-- A predicate on the probe is written as a filter, which `optimize_move_to_prewhere` moves to PREWHERE only after the join
-- order is chosen, or as PREWHERE. The test randomizer changes `optimize_move_to_prewhere`, so the filters pin it.
SELECT '-- runtime filters disabled: the probe read as it is, the merge join reads the hundredth of it that the build side covers';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- the probe filtered on another column: its keys stay spread over its range, assuming independent columns';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + build_narrow.v) FROM (SELECT * FROM probe WHERE v != 12345) AS p JOIN build_narrow ON p.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0, optimize_move_to_prewhere = 0;
SELECT '-- the same with optimize_move_to_prewhere';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + build_narrow.v) FROM (SELECT * FROM probe WHERE v != 12345) AS p JOIN build_narrow ON p.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0, optimize_move_to_prewhere = 1;
SELECT '-- the same written as PREWHERE';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + build_narrow.v) FROM (SELECT * FROM probe PREWHERE v != 12345) AS p JOIN build_narrow ON p.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- the probe filtered on its key: its keys need not be spread over its range, the merge is charged the whole probe';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + build_narrow.v) FROM (SELECT * FROM probe WHERE k % 1000 != 7) AS p JOIN build_narrow ON p.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0, optimize_move_to_prewhere = 0;
SELECT '-- the same with optimize_move_to_prewhere';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + build_narrow.v) FROM (SELECT * FROM probe WHERE k % 1000 != 7) AS p JOIN build_narrow ON p.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0, optimize_move_to_prewhere = 1;
SELECT '-- the same written as PREWHERE';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + build_narrow.v) FROM (SELECT * FROM probe PREWHERE k % 1000 != 7) AS p JOIN build_narrow ON p.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- a read limit that stops the read instead of failing the query: it may drop rows of any column, the merge is charged the whole probe';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0, max_rows_to_read = 1000000, read_overflow_mode = 'break';
SELECT '-- the build side filtered on its key: its keys still lie in its range, the merge reads that hundredth';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + b.v) FROM probe JOIN (SELECT * FROM build_narrow WHERE k % 1000 != 7) AS b ON probe.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;

-- A hash join with the unsorted build side over a tenth of the probe range reads that tenth of the probe through its
-- runtime filter, which beats merging the probe with the sorted `build_wide` first. A probe charged in full is merged.
SELECT '-- with the runtime filter: the probe read as it is, a hash join reads a tenth of it';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN build_tenth_unsorted AS a ON probe.k = a.k JOIN build_wide ON probe.k = build_wide.k);
SELECT '-- the probe filtered on another column: a hash join reads a tenth of it';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(p.v + a.v + build_wide.v) FROM (SELECT * FROM probe WHERE v != 12345) AS p
    JOIN build_tenth_unsorted AS a ON p.k = a.k JOIN build_wide ON p.k = build_wide.k);
SELECT '-- the probe filtered on its key: the hash join is charged the whole probe, which is merge-joined with the sorted build side';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(p.v + a.v + build_wide.v) FROM (SELECT * FROM probe WHERE k % 1000 != 7) AS p
    JOIN build_tenth_unsorted AS a ON p.k = a.k JOIN build_wide ON p.k = build_wide.k);
SELECT '-- the build side filtered on its key: it still bounds the probe, a hash join reads a tenth of it';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT * FROM build_tenth_unsorted WHERE k % 1000 != 7) AS a ON probe.k = a.k
    JOIN build_wide ON probe.k = build_wide.k);
SELECT '-- a build side over the same tenth read with FINAL, which may rewrite values: it does not bound the probe, which is merge-joined';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT * FROM build_tenth_final FINAL) AS a ON probe.k = a.k
    JOIN build_wide ON probe.k = build_wide.k);

SELECT '-- a build side with measured statistics, runtime filters disabled: the merge join reads the few probe keys it covers';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + b.v) FROM probe JOIN build_measured AS b ON probe.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- statistics that miss a part: the range need not enclose the keys, the merge is charged the whole probe';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + b.v) FROM probe JOIN build_partial AS b ON probe.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;

DROP TABLE probe;
DROP TABLE build_narrow;
DROP TABLE build_wide;
DROP TABLE build_tenth_unsorted;
DROP TABLE build_tenth_final;
DROP TABLE build_measured;
DROP TABLE build_partial;
