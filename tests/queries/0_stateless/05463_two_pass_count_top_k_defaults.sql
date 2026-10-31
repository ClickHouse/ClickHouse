-- Tags: no-random-merge-tree-settings, no-parallel-replicas, no-shared-merge-tree, no-shared-catalog
-- no-random-merge-tree-settings, no-shared-merge-tree, no-shared-catalog: the parts written before the ALTERs must stay unmerged and unmutated.

-- A part written before ALTER ADD COLUMN lacks the column, and the reader computes it from its default in every pass.
SET query_plan_two_pass_count_top_k = 1, query_plan_two_pass_count_top_k_min_rows = 0;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0, apply_mutations_on_fly = 0, optimize_aggregation_in_order = 0, serialize_query_plan = 0;
-- Read, sort and speed limits disable the optimization: the test profile sets them all.
SET max_rows_to_read = 0, max_bytes_to_read = 0, max_bytes_to_read_leaf = 0, max_rows_to_group_by = 0, max_rows_to_sort = 0,
    max_bytes_to_sort = 0, max_execution_speed = 0, max_execution_speed_bytes = 0, max_estimated_execution_time = 0;

DROP TABLE IF EXISTS t_two_pass_defaults;
CREATE TABLE t_two_pass_defaults (k UInt64, s String, ts DateTime DEFAULT now(), m UInt64 MATERIALIZED rand() % 50)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_two_pass_defaults;

-- Two parts, 10% of the rows in 20 hot keys with 5 strings each, 30% in 3000 warm keys, 60% scattered over 300000 keys.
INSERT INTO t_two_pass_defaults (k, s) SELECT
    multiIf(number % 10 = 0, intHash64(number) % 20, number % 10 < 4, 1000 + intHash64(number) % 3000, 100000 + intHash64(number) % 300000),
    multiIf(number % 10 = 0, toString(intHash32(number) % 5), number % 10 < 4, toString(intHash32(number) % 50), toString(intHash32(number) % 1000))
FROM numbers(1000000) WHERE number % 2 = 0;
INSERT INTO t_two_pass_defaults (k, s) SELECT
    multiIf(number % 10 = 0, intHash64(number) % 20, number % 10 < 4, 1000 + intHash64(number) % 3000, 100000 + intHash64(number) % 300000),
    multiIf(number % 10 = 0, toString(intHash32(number) % 5), number % 10 < 4, toString(intHash32(number) % 50), toString(intHash32(number) % 1000))
FROM numbers(1000000) WHERE number % 2 = 1;

SELECT 'every part stores the values: applied';
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT m, s, count() FROM t_two_pass_defaults GROUP BY m, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_defaults WHERE ts > '2000-01-01' GROUP BY k, s ORDER BY count() DESC LIMIT 10);

ALTER TABLE t_two_pass_defaults
    ADD COLUMN k_rand UInt64 DEFAULT rand() % 400000,
    ADD COLUMN k_block UInt64 DEFAULT rowNumberInBlock(),
    ADD COLUMN k_stateful UInt64 DEFAULT rowNumberInAllBlocks() % 400000,
    ADD COLUMN k_lambda UInt64 DEFAULT arraySum(arrayMap(x -> x + rand() % 2, [k])),
    ADD COLUMN k_time UInt64 DEFAULT k + toUnixTimestamp64Micro(now64(6)) % 2,
    ADD COLUMN r UInt64 DEFAULT rand() % 2,
    ADD COLUMN k_dependent UInt64 DEFAULT k + r,
    ADD COLUMN k_materialized UInt64 MATERIALIZED k + rand() % 2,
    ADD COLUMN f UInt8 DEFAULT rand() % 2,
    ADD COLUMN g String DEFAULT toString(blockSize() % 3),
    ADD COLUMN k_alias UInt64 ALIAS k + rand() % 2,
    ADD COLUMN k_plus UInt64 DEFAULT k + 1,
    ADD COLUMN s_plus String DEFAULT concat(s, '+'),
    ADD COLUMN ts_added DateTime DEFAULT now();

SELECT 'the old parts lack the column, computed from a default that can change between passes: not applied';
-- The bounding key: random, block-dependent, stateful, random in a lambda, the time, a random default it depends on, MATERIALIZED.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_rand, s, count() FROM t_two_pass_defaults GROUP BY k_rand, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_block, s, count() FROM t_two_pass_defaults GROUP BY k_block, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_stateful, s, count() FROM t_two_pass_defaults GROUP BY k_stateful, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_lambda, s, count() FROM t_two_pass_defaults GROUP BY k_lambda, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_time, s, count() FROM t_two_pass_defaults GROUP BY k_time, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_dependent, s, count() FROM t_two_pass_defaults GROUP BY k_dependent, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_materialized, s, count() FROM t_two_pass_defaults GROUP BY k_materialized, s ORDER BY count() DESC LIMIT 10);
-- A filter, also moved to PREWHERE, and another grouping key.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_defaults WHERE f = 1 GROUP BY k, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_defaults PREWHERE ts_added > '2000-01-01' GROUP BY k, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, g, count() FROM t_two_pass_defaults GROUP BY k, s, g ORDER BY count() DESC LIMIT 10);
-- An ALIAS column is computed in the plan.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_alias, s, count() FROM t_two_pass_defaults GROUP BY k_alias, s ORDER BY count() DESC LIMIT 10);

SELECT 'deterministic defaults, and no default: applied';
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_plus, s, count() FROM t_two_pass_defaults GROUP BY k_plus, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s_plus, count() FROM t_two_pass_defaults GROUP BY k, s_plus ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_defaults GROUP BY k, s ORDER BY count() DESC LIMIT 10);
-- The passes run over the columns the reader computes, also in PREWHERE.
SELECT k_plus, s_plus, count() FROM t_two_pass_defaults WHERE k_plus % 3 != 1 GROUP BY k_plus, s_plus ORDER BY count() DESC, k_plus, s_plus LIMIT 10
SETTINGS log_comment = 'two_pass_defaults_on';
SELECT k_plus, s_plus, count() FROM t_two_pass_defaults WHERE k_plus % 3 != 1 GROUP BY k_plus, s_plus ORDER BY count() DESC, k_plus, s_plus LIMIT 10
SETTINGS query_plan_two_pass_count_top_k = 0;

SELECT 'a new part stores the values, the old parts still lack them: not applied';
INSERT INTO t_two_pass_defaults (k, s) VALUES (1, 'a');
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_rand, s, count() FROM t_two_pass_defaults GROUP BY k_rand, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k_plus, s, count() FROM t_two_pass_defaults GROUP BY k_plus, s ORDER BY count() DESC LIMIT 10);

SELECT 'a pending mutation applied on the fly: not applied';
ALTER TABLE t_two_pass_defaults UPDATE s = concat(s, '!') WHERE k % 7 = 0;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_defaults GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS apply_mutations_on_fly = 1;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_defaults GROUP BY k, s ORDER BY count() DESC LIMIT 10);

SELECT 'a pending ALTER MODIFY COLUMN from Nullable: the reader replaces NULL with the default, not applied for a random one';
DROP TABLE IF EXISTS t_two_pass_modify;
CREATE TABLE t_two_pass_modify (k UInt64, s String, n Nullable(UInt64), d Nullable(UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_two_pass_modify;
INSERT INTO t_two_pass_modify SELECT intHash64(number) % 1000, toString(number % 7), if(number % 3 = 0, NULL, intHash64(number) % 1000),
    if(number % 3 = 0, NULL, intHash64(number) % 1000) FROM numbers(200000);
ALTER TABLE t_two_pass_modify MODIFY COLUMN n UInt64 DEFAULT rand() % 1000, MODIFY COLUMN d UInt64 DEFAULT 7 SETTINGS alter_sync = 0;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT n, s, count() FROM t_two_pass_modify GROUP BY n, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT d, s, count() FROM t_two_pass_modify GROUP BY d, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(d = 7) > 60000 FROM t_two_pass_modify;
DROP TABLE t_two_pass_modify;

SELECT 'a pending DROP COLUMN, then ADD COLUMN of the same name and type: the part keeps the dropped one, not applied';
DROP TABLE IF EXISTS t_two_pass_drop;
CREATE TABLE t_two_pass_drop (k UInt64, s String, c UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_two_pass_drop;
INSERT INTO t_two_pass_drop SELECT intHash64(number) % 1000, toString(number % 7), intHash64(number) % 1000 FROM numbers(200000);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT c, s, count() FROM t_two_pass_drop GROUP BY c, s ORDER BY count() DESC LIMIT 10);
ALTER TABLE t_two_pass_drop DROP COLUMN c SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_two_pass_drop ADD COLUMN c UInt64 DEFAULT rand() % 1000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT c, s, count() FROM t_two_pass_drop GROUP BY c, s ORDER BY count() DESC LIMIT 10);
DROP TABLE t_two_pass_drop;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'] >= 3, ProfileEvents['TwoPassCountTopKFallbacks']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'two_pass_defaults_on';

DROP TABLE t_two_pass_defaults;
