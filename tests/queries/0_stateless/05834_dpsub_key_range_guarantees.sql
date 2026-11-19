-- DPsub's interesting-order costing reads the key range of a sorted probe table and of its build side to estimate how much
-- of the probe the runtime filter of a hash join lets through, or a sharded merge join reads. The two roles need different
-- guarantees of a range: the keys of the build side must lie in its range, and the rows of the probe must be spread over
-- its range, which a range of a column that rows were dropped from by a predicate on the column is not. A range that does
-- not give the guarantee of its role is not used: the probe is then charged in full, and the build side does not bound it.
-- The statistics hints below give the sizes and ranges the plans are chosen for; the hints do not say which columns a
-- filter reads, so a hinted table under a filter counts as filtered on every column. The tables with measured statistics
-- cover what hints cannot describe.

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
DROP TABLE IF EXISTS build_hundredth;
DROP TABLE IF EXISTS build_measured;
DROP TABLE IF EXISTS build_partial;
DROP TABLE IF EXISTS probe_measured;

CREATE TABLE probe (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE build_narrow (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE build_wide (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE build_tenth_unsorted (k Int64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE build_hundredth (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE build_measured (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE build_partial (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE probe_measured (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2', index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO probe SELECT number, number FROM numbers(1000);
INSERT INTO build_narrow SELECT number, number FROM numbers(100);
INSERT INTO build_wide SELECT number * 10, number FROM numbers(100);
INSERT INTO build_tenth_unsorted SELECT number, number FROM numbers(100);
INSERT INTO build_hundredth SELECT number, number FROM numbers(100);
INSERT INTO build_measured SELECT number, number FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
-- A part without statistics: the statistics of the table cover only some of its rows. A merge would add them.
SYSTEM STOP MERGES build_partial;
INSERT INTO build_partial SELECT number, number FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO build_partial SELECT number + 1000, number FROM numbers(10) SETTINGS materialize_statistics_on_insert = 0;
-- A merge join is cheaper than a hash join only for inputs of millions of rows, so a probe with measured statistics needs them.
INSERT INTO probe_measured SELECT number, number % 1000 FROM numbers(5000000) SETTINGS materialize_statistics_on_insert = 1;

-- `build_narrow` covers a hundredth of the probe key range, `build_tenth_unsorted` a tenth and `build_wide` all of it.
-- `build_hundredth` covers a hundredth of the key range of `probe_measured`, `build_measured` 1000 keys of the probe.
SET param__internal_join_table_stat_hints = '{
    "probe": { "cardinality": 200000000, "distinct_keys": { "k": 200000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_narrow": { "cardinality": 2000000, "distinct_keys": { "k": 2000000 }, "key_ranges": { "k": [0, 2000000] } },
    "build_wide": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_tenth_unsorted": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 20000000] } },
    "build_hundredth": { "cardinality": 2500000, "distinct_keys": { "k": 2500000 }, "key_ranges": { "k": [0, 50000] } }
}';

-- Without the runtime filter, only the sharded merge join reads less of the probe when the build keys lie in a narrow range.
SELECT '-- runtime filters disabled: the probe read as it is, the merge join reads the hundredth of it that the build side covers';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- the probe under a filter: its keys need not be spread over its range, the merge is charged the whole probe';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + build_narrow.v) FROM (SELECT * FROM probe WHERE v != 12345) AS p JOIN build_narrow ON p.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- the build side under a filter: its keys still lie in its range, the merge reads that hundredth';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + b.v) FROM probe JOIN (SELECT * FROM build_narrow WHERE v != 12345) AS b ON probe.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;

-- A hash join with the unsorted build side over a tenth of the probe range reads that tenth of the probe through its
-- runtime filter, which beats merging the probe with the sorted `build_wide` first. A probe charged in full is merged.
SELECT '-- with the runtime filter: the probe read as it is, a hash join reads a tenth of it';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN build_tenth_unsorted AS a ON probe.k = a.k JOIN build_wide ON probe.k = build_wide.k);
SELECT '-- the probe under a filter: the hash join is charged the whole probe, which is merge-joined with the sorted build side';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(p.v + a.v + build_wide.v) FROM (SELECT * FROM probe WHERE v != 12345) AS p
    JOIN build_tenth_unsorted AS a ON p.k = a.k JOIN build_wide ON p.k = build_wide.k);
SELECT '-- the build side under a filter: it still bounds the probe, a hash join reads a tenth of it';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT * FROM build_tenth_unsorted WHERE v != 12345) AS a ON probe.k = a.k
    JOIN build_wide ON probe.k = build_wide.k);

SELECT '-- a build side with measured statistics, runtime filters disabled: the merge join reads the few probe keys it covers';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + b.v) FROM probe JOIN build_measured AS b ON probe.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- the same filtered on another column: its keys still lie in its range';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + b.v) FROM probe JOIN (SELECT * FROM build_measured WHERE v != 12345) AS b ON probe.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- statistics that miss a part: the range need not enclose the keys, the merge is charged the whole probe';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + b.v) FROM probe JOIN build_partial AS b ON probe.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;

SELECT '-- a probe with measured statistics, runtime filters disabled: the merge join reads the hundredth the build side covers';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + b.v) FROM probe_measured AS p JOIN build_hundredth AS b ON p.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- the same filtered on another column: its keys stay spread over its range, assuming independent columns';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + b.v) FROM (SELECT * FROM probe_measured WHERE v != 7) AS p JOIN build_hundredth AS b ON p.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;
SELECT '-- the same filtered on its key: the merge is charged the whole probe';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT sum(p.v + b.v) FROM (SELECT * FROM probe_measured WHERE k % 1000 != 7) AS p JOIN build_hundredth AS b ON p.k = b.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;

DROP TABLE probe;
DROP TABLE build_narrow;
DROP TABLE build_wide;
DROP TABLE build_tenth_unsorted;
DROP TABLE build_hundredth;
DROP TABLE build_measured;
DROP TABLE build_partial;
DROP TABLE probe_measured;
