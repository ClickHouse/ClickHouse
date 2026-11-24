-- Tags: no-parallel-replicas
-- no-parallel-replicas: the reads of parallel replicas are not pruned.

-- Granules whose values of the first primary key column cannot reach a HAVING count() lower bound are skipped
-- before reading; the result must not change.

-- The passes refuse a serialized plan, so pin the setting for the distributed-plan suite.
SET serialize_query_plan = 0;
SET query_plan_enable_optimizations = 1;
SET query_plan_merge_filters = 1;
SET query_plan_aggregation_having_prefilter = 1;
-- An aggregation in order or past max_rows_to_group_by gets no HAVING bound.
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
-- One part per INSERT.
SET max_insert_threads = 1, max_block_size = 65536, min_insert_block_size_rows = 1000000, max_insert_block_size = 1000000;

DROP ROW POLICY IF EXISTS p_having_pk_mv_definer ON mv_having_pk_definer;
DROP VIEW IF EXISTS mv_having_pk_definer;
DROP ROW POLICY IF EXISTS p_having_pk_mv ON mv_having_pk;
DROP VIEW IF EXISTS mv_having_pk;
DROP VIEW IF EXISTS v_having_pk;
DROP TABLE IF EXISTS t_having_pk_mv_dst;
DROP TABLE IF EXISTS t_having_pk_default;
DROP ROW POLICY IF EXISTS p_having_pk ON t_having_pk;
DROP TABLE IF EXISTS t_having_pk;
CREATE TABLE t_having_pk (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_having_pk;

-- Part 1: keys 0..999, every hundredth key has 50000 rows, the others 200.
INSERT INTO t_having_pk SELECT k, k * 100000 + n FROM (SELECT number AS k FROM numbers(1000)) ARRAY JOIN range(if(k % 100 = 0, 50000, 200)) AS n;
-- Part 2: keys 500..1499 overlap part 1; every 250th key has 20000 rows, the others 20.
INSERT INTO t_having_pk SELECT k, k * 100000 + n FROM (SELECT 500 + number AS k FROM numbers(1000)) ARRAY JOIN range(if(k % 250 = 0, 20000, 20)) AS n;

SELECT 'thresholds';
SELECT 'gt 0', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 0) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'gt 0', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 0) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT 'gt 220', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 220) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'gt 220', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 220) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT 'gt 20000', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'gt 20000', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT 'ge 20200', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() >= 20200) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'ge 20200', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() >= 20200) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT 'gt 20200', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 20200) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'gt 20200', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 20200) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT 'le 50020', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING 50020 <= count()) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'le 50020', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING 50020 <= count()) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT 'gt 50020', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 50020) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'gt 50020', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 50020) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT 'gt 70000', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 70000) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT 'gt 70000', count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 70000) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'exact bound';
DROP TABLE IF EXISTS t_having_pk_exact;
CREATE TABLE t_having_pk_exact (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4096, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
-- Key 0 fills its granules, so its bound equals its count.
INSERT INTO t_having_pk_exact SELECT if(number < 12288, 0, 1 + intDiv(number - 12288, 10)) FROM numbers(53248);
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_exact GROUP BY k HAVING count() > 12287) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_exact GROUP BY k HAVING count() > 12287) SETTINGS query_plan_aggregation_having_count_pruning = 1;
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_exact GROUP BY k HAVING count() >= 12288) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_exact GROUP BY k HAVING count() >= 12288) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'where and conjunction';
SELECT count(), sum(k), sum(c), sum(sv) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_having_pk WHERE v % 3 != 1 GROUP BY k HAVING count() > 13000 AND sum(v) > 0) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(k), sum(c), sum(sv) FROM (SELECT k, count() AS c, sum(v) AS sv FROM t_having_pk WHERE v % 3 != 1 GROUP BY k HAVING count() > 13000 AND sum(v) > 0) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'common key range';
DROP TABLE IF EXISTS t_having_pk_common;
CREATE TABLE t_having_pk_common (k UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4096, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_having_pk_common;
-- Every value in [2, 3] can reach the bound, so only the granules reaching out of it are swept; the first granule of
-- the first part reaches into it with the rows of key 2.
INSERT INTO t_having_pk_common SELECT multiIf(number < 4000, 1, number < 24000, 2, 3) FROM numbers(32768);
INSERT INTO t_having_pk_common SELECT if(number < 20000, 2, 3) FROM numbers(32768);
INSERT INTO t_having_pk_common SELECT if(number < 20480, 2, 3) FROM numbers(32768);
SELECT count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_common GROUP BY k HAVING count() > 10000) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_common GROUP BY k HAVING count() > 10000) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'key condition with two ranges';
SELECT count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk WHERE k NOT BETWEEN 300 AND 650 GROUP BY k HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk WHERE k NOT BETWEEN 300 AND 650 GROUP BY k HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'two keys';
SELECT count(), sum(k), sum(c) FROM (SELECT k, v % 2 AS p, count() AS c FROM t_having_pk GROUP BY k, p HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(k), sum(c) FROM (SELECT k, v % 2 AS p, count() AS c FROM t_having_pk GROUP BY k, p HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'no primary key column among the keys';
SELECT count(), sum(c) FROM (SELECT v % 7 AS p, count() AS c FROM t_having_pk GROUP BY p HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'disjunction';
SELECT count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk GROUP BY k HAVING count() > 50000 OR k = 7) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'string key';
DROP TABLE IF EXISTS t_having_pk_s;
-- Adaptive granularity: the last granule of a part ends at the part's last key.
CREATE TABLE t_having_pk_s (s String, v UInt64) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 8192;
SYSTEM STOP MERGES t_having_pk_s;
INSERT INTO t_having_pk_s SELECT toString(k), k FROM t_having_pk WHERE k < 1000;
INSERT INTO t_having_pk_s SELECT toString(k), k FROM t_having_pk WHERE k >= 1000;
SELECT count(), sum(length(s)), sum(c) FROM (SELECT s, count() AS c FROM t_having_pk_s GROUP BY s HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(length(s)), sum(c) FROM (SELECT s, count() AS c FROM t_having_pk_s GROUP BY s HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'low cardinality key';
DROP TABLE IF EXISTS t_having_pk_lc;
CREATE TABLE t_having_pk_lc (s LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY s
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_having_pk_lc;
INSERT INTO t_having_pk_lc SELECT toString(k), k FROM t_having_pk WHERE k < 1000;
INSERT INTO t_having_pk_lc SELECT toString(k), k FROM t_having_pk WHERE k >= 1000;
SELECT count(), sum(length(s)), sum(c) FROM (SELECT s, count() AS c FROM t_having_pk_lc GROUP BY s HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(length(s)), sum(c) FROM (SELECT s, count() AS c FROM t_having_pk_lc GROUP BY s HAVING count() > 20000) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'descending key';
DROP TABLE IF EXISTS t_having_pk_desc;
CREATE TABLE t_having_pk_desc (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k DESC
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_having_pk_desc;
INSERT INTO t_having_pk_desc SELECT k, v FROM t_having_pk WHERE k < 1000;
INSERT INTO t_having_pk_desc SELECT k, v FROM t_having_pk WHERE k >= 500;
SELECT count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_desc GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 0;
SELECT count(), sum(k), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_desc GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 1;

SELECT 'key expression named like a column';
DROP TABLE IF EXISTS t_having_pk_named;
-- The index holds the key expression, not the stored column of that name: https://github.com/ClickHouse/ClickHouse/issues/133178
-- The split-ranges injection computes the sorting key over the read block, which replaces the stored column of that name.
CREATE TABLE t_having_pk_named (x UInt32, `negate(x)` Int64) ENGINE = MergeTree ORDER BY negate(x)
SETTINGS allow_suspicious_indices = 1, index_granularity = 4096, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_having_pk_named SELECT number, 0 FROM numbers(53248);
ALTER TABLE t_having_pk_named UPDATE `negate(x)` = 0 WHERE 1 SETTINGS mutations_sync = 2;
SELECT count(), sum(c) FROM (SELECT `negate(x)`, count() AS c FROM t_having_pk_named GROUP BY `negate(x)` HAVING count() > 10000) SETTINGS query_plan_aggregation_having_count_pruning = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SELECT count(), sum(c) FROM (SELECT `negate(x)`, count() AS c FROM t_having_pk_named GROUP BY `negate(x)` HAVING count() > 10000) SETTINGS query_plan_aggregation_having_count_pruning = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

SELECT 'query condition cache';
-- HAVING-skipped marks are never stored as "no rows match the WHERE".
SELECT count() FROM (SELECT k FROM t_having_pk WHERE v % 3 != 1 GROUP BY k HAVING count() > 13000) SETTINGS use_query_condition_cache = 1, query_plan_aggregation_having_count_pruning = 1;
SELECT count() FROM t_having_pk WHERE v % 3 != 1 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM t_having_pk WHERE v % 3 != 1 SETTINGS use_query_condition_cache = 0;

SELECT 'pruned rows';
-- The split-ranges injection reads some rows twice, so read_rows would vary between runs of the same query.
SELECT count() FROM (SELECT k FROM t_having_pk GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_on';
SELECT count() FROM (SELECT k FROM t_having_pk GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_off';
SELECT count() FROM (SELECT k FROM t_having_pk GROUP BY k HAVING count() > 1) SETTINGS query_plan_aggregation_having_count_pruning = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_low';
SELECT count() FROM (SELECT k FROM t_having_pk_desc GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_desc_on';
SELECT count() FROM (SELECT k FROM t_having_pk_desc GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_desc_off';
SELECT count() FROM (SELECT s FROM t_having_pk_lc GROUP BY s HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_lc_on';
SELECT count() FROM (SELECT s FROM t_having_pk_lc GROUP BY s HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_lc_off';
-- Under a row policy the granules' row counts include the hidden rows, so the read is not pruned.
CREATE ROW POLICY p_having_pk ON t_having_pk FOR SELECT USING v % 2 = 0 TO CURRENT_USER;
SELECT count() FROM (SELECT k FROM t_having_pk GROUP BY k HAVING count() > 24999) SETTINGS query_plan_aggregation_having_count_pruning = 1, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_policy_on';
SELECT count() FROM (SELECT k FROM t_having_pk GROUP BY k HAVING count() > 24999) SETTINGS query_plan_aggregation_having_count_pruning = 0, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_policy_off';
DROP ROW POLICY p_having_pk ON t_having_pk;
-- A row policy applied above the read is out of its sight: reads inside a view are not pruned, nor the reads of a
-- query that applies a row policy (here one that a materialized view with a converted column applies above the read,
-- not as the read's row-level filter), nor the reads of a SQL SECURITY DEFINER body, whose query context is its own.
CREATE VIEW v_having_pk AS SELECT k, v FROM t_having_pk;
SELECT count() FROM (SELECT k FROM v_having_pk GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_view_on';
SELECT count() FROM (SELECT k FROM v_having_pk GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_view_off';
CREATE TABLE t_having_pk_mv_dst (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_having_pk_mv_dst SELECT k, v FROM t_having_pk;
CREATE MATERIALIZED VIEW mv_having_pk TO t_having_pk_mv_dst (k UInt32, v UInt32) AS SELECT k, toUInt32(v) AS v FROM t_having_pk;
CREATE ROW POLICY p_having_pk_mv ON mv_having_pk FOR SELECT USING v % 2 = 0 TO CURRENT_USER;
SELECT count() FROM (SELECT k FROM mv_having_pk GROUP BY k HAVING count() > 24999) SETTINGS query_plan_aggregation_having_count_pruning = 1, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_mv_policy_on';
SELECT count() FROM (SELECT k FROM mv_having_pk GROUP BY k HAVING count() > 24999) SETTINGS query_plan_aggregation_having_count_pruning = 0, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_mv_policy_off';
DROP ROW POLICY p_having_pk_mv ON mv_having_pk;
DROP VIEW mv_having_pk;
CREATE MATERIALIZED VIEW mv_having_pk_definer TO t_having_pk_mv_dst (k UInt32, v UInt32) DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, toUInt32(v) AS v FROM t_having_pk;
CREATE ROW POLICY p_having_pk_mv_definer ON mv_having_pk_definer FOR SELECT USING v % 2 = 0 TO CURRENT_USER;
SELECT count() FROM (SELECT k FROM mv_having_pk_definer GROUP BY k HAVING count() > 24999) SETTINGS query_plan_aggregation_having_count_pruning = 1, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_mv_definer_on';
SELECT count() FROM (SELECT k FROM mv_having_pk_definer GROUP BY k HAVING count() > 24999) SETTINGS query_plan_aggregation_having_count_pruning = 0, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_mv_definer_off';
DROP ROW POLICY p_having_pk_mv_definer ON mv_having_pk_definer;
DROP VIEW mv_having_pk_definer;
DROP TABLE t_having_pk_mv_dst;
DROP VIEW v_having_pk;
-- The reader computes the default of a column a part lacks over the rows it reads, so that read is not pruned.
CREATE TABLE t_having_pk_default (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_having_pk_default SELECT if(number < 81920, intDiv(number, 8192), 10) FROM numbers(131072);
ALTER TABLE t_having_pk_default ADD COLUMN v UInt64 DEFAULT rowNumberInBlock();
SELECT count() FROM (SELECT k FROM t_having_pk_default GROUP BY k HAVING count() > 20000 AND sum(v) >= 0) SETTINGS query_plan_aggregation_having_count_pruning = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_default_on';
SELECT count() FROM (SELECT k FROM t_having_pk_default GROUP BY k HAVING count() > 20000 AND sum(v) >= 0) SETTINGS query_plan_aggregation_having_count_pruning = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0, log_comment = '05500_prune_default_off';
SYSTEM FLUSH LOGS query_log;
SELECT
    sumIf(read_rows, log_comment = '05500_prune_on') < sumIf(read_rows, log_comment = '05500_prune_off'),
    sumIf(read_rows, log_comment = '05500_prune_low') = sumIf(read_rows, log_comment = '05500_prune_off'),
    sumIf(read_rows, log_comment = '05500_prune_desc_on') = sumIf(read_rows, log_comment = '05500_prune_desc_off'),
    sumIf(read_rows, log_comment = '05500_prune_lc_on') < sumIf(read_rows, log_comment = '05500_prune_lc_off'),
    sumIf(read_rows, log_comment = '05500_prune_policy_on') = sumIf(read_rows, log_comment = '05500_prune_policy_off'),
    sumIf(read_rows, log_comment = '05500_prune_view_on') = sumIf(read_rows, log_comment = '05500_prune_view_off'),
    sumIf(read_rows, log_comment = '05500_prune_mv_policy_on') = sumIf(read_rows, log_comment = '05500_prune_mv_policy_off'),
    sumIf(read_rows, log_comment = '05500_prune_mv_definer_on') = sumIf(read_rows, log_comment = '05500_prune_mv_definer_off'),
    sumIf(read_rows, log_comment = '05500_prune_default_on') = sumIf(read_rows, log_comment = '05500_prune_default_off')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05500_prune_%';

SELECT 'cold primary index';
DROP TABLE IF EXISTS t_having_pk_cold;
CREATE TABLE t_having_pk_cold (k UInt32, v UInt64, INDEX iv v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, primary_key_lazy_load = 1, use_primary_key_cache = 0, prewarm_primary_key_cache = 0;
SYSTEM STOP MERGES t_having_pk_cold;
INSERT INTO t_having_pk_cold SELECT k, v FROM t_having_pk;
SYSTEM UNLOAD PRIMARY KEY t_having_pk_cold;
-- The skip index selects 7 of the part's 98 marks: their read must not load the whole primary index, a full read does.
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_having_pk_cold WHERE v < 1000000 GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SELECT sum(primary_key_bytes_in_memory) = 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_having_pk_cold' AND active;
SELECT count() FROM (SELECT k FROM t_having_pk_cold GROUP BY k HAVING count() > 49999) SETTINGS query_plan_aggregation_having_count_pruning = 1, use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SELECT sum(primary_key_bytes_in_memory) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_having_pk_cold' AND active;

DROP TABLE t_having_pk;
DROP TABLE t_having_pk_exact;
DROP TABLE t_having_pk_common;
DROP TABLE t_having_pk_s;
DROP TABLE t_having_pk_lc;
DROP TABLE t_having_pk_desc;
DROP TABLE t_having_pk_named;
DROP TABLE t_having_pk_cold;
DROP TABLE t_having_pk_default;
