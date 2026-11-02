-- The plan marking of `enable_join_seal_gated_reading` (behaviour documented with that setting
-- in src/Core/Settings.cpp): the probe side of a hash JOIN is read through a gate that opens
-- when the build-side runtime filter (`enable_join_runtime_filters`) is complete, and the filter
-- then prunes whole mark ranges by the primary key. A probe read is gated only when the filters
-- applied to it cover a PREFIX of its primary key, are built by this very join, and are
-- guaranteed to become a positive predicate. Every query below asserts its result (the marking
-- must never change it) and, where the plan shape is the point, whether the probe ended up gated.
-- The read-volume effect of the gating on a large table is covered by
-- 04653_join_seal_gated_reading; this test is the eligibility matrix and stays tiny.

-- Runtime filters, and therefore the gating, exist only with the analyzer.
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1, enable_join_seal_gated_reading = 1;
-- The tables are tiny: build the runtime filter regardless of the estimated probe size.
SET join_runtime_filter_min_probe_rows = 0;
-- The read-time index analysis of the same filter prunes on its own; keep it out of the picture.
SET enable_join_runtime_filters_index_analysis = 0;
-- Only plain local reads are gated: no parallel replicas, no join sharded by primary key ranges.
SET enable_parallel_replicas = 0, query_plan_join_shard_by_pk_ranges = 0;
-- Keep the indexed table on the probe side, and pin the pipeline that wires the seal
-- (build the right side first), which is the hash join's.
SET query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t_sgm_probe;
DROP TABLE IF EXISTS t_sgm_build;
DROP TABLE IF EXISTS t_sgm_final;
DROP TABLE IF EXISTS t_sgm_ab;
DROP TABLE IF EXISTS t_sgm_build_ab;

-- Every table pins auto_statistics_types = '': the statistics refresh scheduled after an insert
-- adds background activity, and nothing here uses statistics.
CREATE TABLE t_sgm_probe (k UInt64, v String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 4, auto_statistics_types = '';
INSERT INTO t_sgm_probe SELECT number, toString(number) FROM numbers(400);
CREATE TABLE t_sgm_build (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
-- 4 keys spread over the probe primary key range: most of the 100 marks are prunable.
INSERT INTO t_sgm_build SELECT number * 100 FROM numbers(4);

-- An eligible join: the filter on the single primary key column covers the whole key.
SELECT count(), sum(p.k) FROM t_sgm_probe AS p JOIN t_sgm_build AS b ON p.k = b.k;
-- The gated result equals the ungated one.
SELECT count(), sum(p.k) FROM t_sgm_probe AS p JOIN t_sgm_build AS b ON p.k = b.k
    SETTINGS enable_join_seal_gated_reading = 0;
SELECT count() > 0 AS gated FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_sgm_probe AS p JOIN t_sgm_build AS b ON p.k = b.k
) WHERE explain LIKE '%SealGatedRead%';

-- Only the hash-join pipeline wires the seal: a merge join fills both sides at once.
SELECT count(), sum(p.k) FROM t_sgm_probe AS p JOIN t_sgm_build AS b ON p.k = b.k
    SETTINGS join_algorithm = 'full_sorting_merge';

-- The probe side must be a plain merge-tree read: a table function has no primary key to prune.
SELECT count() FROM numbers(400) AS n JOIN t_sgm_build AS b ON n.number = b.k;
SELECT count() > 0 AS gated_table_function FROM (
    EXPLAIN PIPELINE SELECT count() FROM numbers(400) AS n JOIN t_sgm_build AS b ON n.number = b.k
) WHERE explain LIKE '%SealGatedRead%';

-- A join on an expression of the key registers no filter on the read: nothing to gate on.
SELECT count() FROM t_sgm_probe AS p JOIN t_sgm_build AS b ON cityHash64(p.k) = cityHash64(b.k);
SELECT count() > 0 AS gated_expression_key FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_sgm_probe AS p JOIN t_sgm_build AS b ON cityHash64(p.k) = cityHash64(b.k)
) WHERE explain LIKE '%SealGatedRead%';

-- An ANTI join builds a NOT-contains filter, which can never prune positively.
SELECT count() FROM t_sgm_probe AS p LEFT ANTI JOIN t_sgm_build AS b ON p.k = b.k;
SELECT count() > 0 AS gated_anti FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_sgm_probe AS p LEFT ANTI JOIN t_sgm_build AS b ON p.k = b.k
) WHERE explain LIKE '%SealGatedRead%';

-- FINAL reads the parts through a merging transform, which the gate does not cross.
CREATE TABLE t_sgm_final (k UInt64, v String) ENGINE = ReplacingMergeTree ORDER BY k
    SETTINGS index_granularity = 4, auto_statistics_types = '';
INSERT INTO t_sgm_final SELECT number, toString(number) FROM numbers(400);
SELECT count(), sum(p.k) FROM t_sgm_final AS p FINAL JOIN t_sgm_build AS b ON p.k = b.k;
SELECT count() > 0 AS gated_final FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_sgm_final AS p FINAL JOIN t_sgm_build AS b ON p.k = b.k
) WHERE explain LIKE '%SealGatedRead%';

CREATE TABLE t_sgm_ab (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS index_granularity = 4, auto_statistics_types = '';
INSERT INTO t_sgm_ab SELECT number % 20, intDiv(number, 20) FROM numbers(400);
CREATE TABLE t_sgm_build_ab (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b)
    SETTINGS auto_statistics_types = '';
INSERT INTO t_sgm_build_ab SELECT number * 5, number FROM numbers(4);

-- Both key columns joined (written in the reverse order, so the filter on the trailing column is
-- examined first) cover the (a, b) prefix: the read is gated.
SELECT count(), sum(p.a) FROM t_sgm_ab AS p JOIN t_sgm_build_ab AS q ON p.b = q.b AND p.a = q.a;
SELECT count() > 0 AS gated_pk_prefix FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_sgm_ab AS p JOIN t_sgm_build_ab AS q ON p.b = q.b AND p.a = q.a
) WHERE explain LIKE '%SealGatedRead%';

-- A filter on the trailing key column alone selects rows scattered over the whole part and covers
-- no prefix: that probe is not gated, and its result stays correct.
SELECT count() FROM t_sgm_ab AS p JOIN t_sgm_build_ab AS q ON p.b = q.b;
SELECT count() > 0 AS gated_non_prefix FROM (
    EXPLAIN PIPELINE SELECT count() FROM t_sgm_ab AS p JOIN t_sgm_build_ab AS q ON p.b = q.b
) WHERE explain LIKE '%SealGatedRead%';

DROP TABLE t_sgm_probe;
DROP TABLE t_sgm_build;
DROP TABLE t_sgm_final;
DROP TABLE t_sgm_ab;
DROP TABLE t_sgm_build_ab;
