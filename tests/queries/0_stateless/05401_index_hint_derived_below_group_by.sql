-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/124502: a derived hint still prunes below GROUP BY and LIMIT BY

SET optimize_and_compare_chain = 1;
SET query_plan_filter_push_down = 1;
SET use_query_condition_cache = 0;
SET optimize_or_like_chain = 1;

DROP TABLE IF EXISTS t_chain_keys;
CREATE TABLE t_chain_keys (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
INSERT INTO t_chain_keys SELECT intDiv(number, 100), number % 997 FROM numbers(100000);
OPTIMIZE TABLE t_chain_keys FINAL;

-- the derived `indexHint(a < 10)` is the only condition on the key
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a, b FROM t_chain_keys GROUP BY a, b HAVING a < b AND b < 10) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM (SELECT a, b FROM t_chain_keys GROUP BY a, b HAVING a < b AND b < 10);

-- a derived hint inside an OR does not keep the OR above the aggregation
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a, b FROM t_chain_keys GROUP BY a, b HAVING (b < a AND a < 10) OR a = 500) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM (SELECT a, b FROM t_chain_keys GROUP BY a, b HAVING (b < a AND a < 10) OR a = 500);

-- the conjuncts the hint is derived from cannot be pushed
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a, max(b) AS m FROM t_chain_keys GROUP BY a HAVING a < m AND m < 120) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM (SELECT a, max(b) AS m FROM t_chain_keys GROUP BY a HAVING a < m AND m < 120);

SELECT countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM (SELECT a, b FROM t_chain_keys ORDER BY a, b LIMIT 1 BY a, b) WHERE (b < a AND a < 10) OR a = 500);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT a, b FROM t_chain_keys ORDER BY a, b LIMIT 1 BY a, b) WHERE (b < a AND a < 10) OR a = 500) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM (SELECT a, b FROM t_chain_keys ORDER BY a, b LIMIT 1 BY a, b) WHERE (b < a AND a < 10) OR a = 500;

-- a derived hint from an outer WHERE prunes through a GROUP BY subquery
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT a, b FROM t_chain_keys GROUP BY a, b) WHERE a < b AND b < 10) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM (SELECT a, b FROM t_chain_keys GROUP BY a, b) WHERE a < b AND b < 10;

-- a hint the OR-LIKE chain rewrite adds does not keep the OR above the aggregation
SELECT countIf(explain LIKE '%multiSearchAny(%') > 0 AND countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT a, b FROM t_chain_keys GROUP BY a, b HAVING (a < 10 AND (toString(b) LIKE '%1%' OR toString(b) LIKE '%2%' OR toString(b) LIKE '%3%' OR toString(b) LIKE '%4%')) OR a = 500);
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a, b FROM t_chain_keys GROUP BY a, b HAVING (a < 10 AND (toString(b) LIKE '%1%' OR toString(b) LIKE '%2%' OR toString(b) LIKE '%3%' OR toString(b) LIKE '%4%')) OR a = 500) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM (SELECT a, b FROM t_chain_keys GROUP BY a, b HAVING (a < 10 AND (toString(b) LIKE '%1%' OR toString(b) LIKE '%2%' OR toString(b) LIKE '%3%' OR toString(b) LIKE '%4%')) OR a = 500);

DROP TABLE t_chain_keys;

-- a user-written hint equal to a derived one still must not prune below GROUP BY
DROP TABLE IF EXISTS t_hint_key;
CREATE TABLE t_hint_key (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_hint_key SELECT intDiv(number, 10), number % 10 FROM numbers(100);
OPTIMIZE TABLE t_hint_key FINAL;

SELECT count() FROM (SELECT k, v FROM t_hint_key GROUP BY k, v HAVING ((k < v AND v < 3) OR v = 7) AND indexHint(k < 3));
SELECT count() FROM (SELECT k, v FROM t_hint_key GROUP BY k, v HAVING indexHint(k < 3) AND ((k < v AND v < 3) OR v = 7));
-- a hint factored out of an OR keeps a user copy's missing mark
SELECT countIf(explain LIKE '%WHERE and(indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM (SELECT k, v FROM remote('127.0.0.1', currentDatabase(), t_hint_key) GROUP BY k, v) WHERE (v = 7 AND indexHint(k < 3)) OR (k < v AND v < 3) SETTINGS optimize_extract_common_expressions = 1, prefer_localhost_replica = 1);
SELECT count() FROM (SELECT k, v FROM remote('127.0.0.1', currentDatabase(), t_hint_key) GROUP BY k, v) WHERE (v = 7 AND indexHint(k < 3)) OR (k < v AND v < 3) SETTINGS optimize_extract_common_expressions = 1, prefer_localhost_replica = 1;

DROP TABLE t_hint_key;

-- below GROUP BY in a remote() local plan the derived hint still means the alias, not the column it shadows
DROP TABLE IF EXISTS t_hint_alias;
CREATE TABLE t_hint_alias (k UInt32, v UInt32, w UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_hint_alias SELECT number, number % 10, number % 7, [number % 7, number % 10] FROM numbers(100);
OPTIMIZE TABLE t_hint_alias FINAL;
SELECT countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM (SELECT w AS k, v FROM remote('127.0.0.1', currentDatabase(), t_hint_alias) AS t GROUP BY w, v, t.k) WHERE k < v AND v < 3 SETTINGS prefer_localhost_replica = 1);
SELECT count() FROM (SELECT w AS k, v FROM remote('127.0.0.1', currentDatabase(), t_hint_alias) AS t GROUP BY w, v, t.k) WHERE k < v AND v < 3 SETTINGS prefer_localhost_replica = 1;
-- a derived hint that crosses ARRAY JOIN is dropped, not bound to the array of the same name
SELECT countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM (SELECT x, v FROM t_hint_alias AS t ARRAY JOIN [w, v] AS x) WHERE x < v AND v < 3);
SELECT count() FROM (SELECT x, v FROM t_hint_alias AS t ARRAY JOIN [w, v] AS x) WHERE x < v AND v < 3;
SELECT countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM (SELECT arr, v FROM t_hint_alias ARRAY JOIN arr GROUP BY arr, v) WHERE arr < v AND v < 3);
SELECT count() FROM (SELECT arr, v FROM t_hint_alias ARRAY JOIN arr GROUP BY arr, v) WHERE arr < v AND v < 3;
DROP TABLE t_hint_alias;

-- a derived hint alone below GROUP BY does not pass GROUPING SETS that lack its key
DROP TABLE IF EXISTS t_hint_gs;
CREATE TABLE t_hint_gs (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_hint_gs SELECT number, number FROM numbers(200);
OPTIMIZE TABLE t_hint_gs FINAL;
SELECT countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM (SELECT a, max(b) AS m FROM (SELECT a, b FROM t_hint_gs GROUP BY GROUPING SETS ((a, b), (b))) GROUP BY a HAVING a < m AND m < 120));
SELECT count() FROM (SELECT a, max(b) AS m FROM (SELECT a, b FROM t_hint_gs GROUP BY GROUPING SETS ((a, b), (b))) GROUP BY a HAVING a < m AND m < 120);
-- nor below totals, which are not filtered
SELECT countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM (SELECT a, max(b) AS m FROM t_hint_gs GROUP BY a WITH TOTALS) WHERE a < m AND m < 120);
SELECT * FROM (SELECT a, max(b) AS m FROM t_hint_gs GROUP BY a WITH TOTALS) WHERE a < m AND m < 120;
DROP TABLE t_hint_gs;
