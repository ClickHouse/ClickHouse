-- DPsub's interesting-order costing reads the key ranges of a build side to estimate how much of a sorted probe side the
-- runtime filter of a hash join prunes, or a sharded merge join reads. The range of a column does not describe a
-- value-changing function of it, e.g. `negate(k)`: kept under the name of the function, the range of `k` would make DPsub
-- see no overlap with the probe, price the probe of the hash join at nothing and pick hash joins everywhere, where the
-- equivalent build sides with keys over the whole probe range get a merge join.
-- The tables are small; the statistics hints below give the sizes and ranges the plans are chosen for.

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
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;
-- A merge join of sorted inputs is costed as sharded by primary key ranges only with this on.
SET query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build_wide;
DROP TABLE IF EXISTS build_wide2;
DROP TABLE IF EXISTS build_tenth;
DROP TABLE IF EXISTS build_narrow;
DROP TABLE IF EXISTS build_negated;

CREATE TABLE probe (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_wide (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_wide2 (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_tenth (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_narrow (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_negated (k Int64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO probe SELECT number, number FROM numbers(1000);
INSERT INTO build_wide SELECT number * 10, number FROM numbers(100);
INSERT INTO build_wide2 SELECT number * 10, number FROM numbers(100);
INSERT INTO build_tenth SELECT number, number FROM numbers(100);
INSERT INTO build_narrow SELECT number, number FROM numbers(100);
INSERT INTO build_negated SELECT -toInt64(number) * 10, number FROM numbers(100);

-- The keys of `build_negated` are the negated keys of `build_wide`, so they cover the whole probe range; `build_tenth`
-- covers a tenth of it and `build_narrow` a hundredth.
SET param__internal_join_table_stat_hints = '{
    "probe": { "cardinality": 200000000, "distinct_keys": { "k": 200000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_wide": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_wide2": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_tenth": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 20000000] } },
    "build_narrow": { "cardinality": 2000000, "distinct_keys": { "k": 2000000 }, "key_ranges": { "k": [0, 2000000] } },
    "build_negated": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [-200000000, 0] } }
}';

-- The probe joins two build sides on the same key. A merge join of the probe with `build_wide` beats a hash join that
-- reads the whole probe, but not one whose runtime filter prunes most of it. A build side behind an expression or a
-- subquery is not sorted any more, so it can only be hash-joined, and a range that makes its hash join look cheap
-- removes the merge join from the plan.

SELECT '-- an alias of a build key over the whole probe range: the probe is merge-joined';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, v FROM build_wide2) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k);

SELECT '-- the negated keys cover the whole probe range too, and the range of `k` says nothing about them: as with the alias';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT negate(k) AS nk, v FROM build_negated) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k);

SELECT '-- a cast that happens to keep every value: no range either, and the same plan as with one over the whole range';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT toInt64(toInt32(k)) AS nk, v FROM build_wide2) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k);

-- The hinted ranges are not measured statistics, so they do not survive the steps that used to drop every range: a build
-- side read through `GROUP BY`, `ORDER BY ... LIMIT` or `LIMIT` is costed like one with unknown keys, as before.
SELECT '-- a build side over a tenth of the probe range, read directly: its range is used, a merge join reads that tenth';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, v FROM build_tenth) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k);
SELECT '-- the same build side behind GROUP BY, ORDER BY LIMIT and LIMIT: its hinted range is dropped, as with an alias over the whole range';
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, any(v) AS v FROM build_tenth GROUP BY k) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k);
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, v FROM build_tenth ORDER BY v LIMIT 20000000) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k);
SELECT if(countIf(explain LIKE '%FullSortingMergeJoin%') > 0, 'uses a merge join', 'hash joins only') FROM (EXPLAIN PLAN
    SELECT sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, v FROM build_tenth LIMIT 20000000) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k);

-- Without the runtime filter, only the sharded merge join reads less of the probe side when the build keys lie in a narrow
-- range, so the merge join is chosen only with the range: the hinted range of a directly read table is still used.
SELECT '-- a sorted build side over a hundredth of the probe range, runtime filters disabled: a merge join reads that hundredth';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;

SELECT '-- the results do not depend on the plan';
SELECT count(), sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, v FROM build_wide2) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k;
SELECT count(), sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT negate(k) AS nk, v FROM build_negated) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k;
SELECT count(), sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT toInt64(toInt32(k)) AS nk, v FROM build_wide2) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k;
SELECT count(), sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, v FROM build_tenth) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k;
SELECT count(), sum(probe.v + a.v + build_wide.v) FROM probe JOIN (SELECT k AS nk, any(v) AS v FROM build_tenth GROUP BY k) AS a ON probe.k = a.nk JOIN build_wide ON probe.k = build_wide.k;
SELECT count(), sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k SETTINGS enable_join_runtime_filters = 0;

DROP TABLE probe;
DROP TABLE build_wide;
DROP TABLE build_wide2;
DROP TABLE build_tenth;
DROP TABLE build_narrow;
DROP TABLE build_negated;
