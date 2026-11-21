-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/128071: a `set` index evaluates a derived hint on the column the hint names, not on the one it reads

SET optimize_and_compare_chain = 1;
SET optimize_or_like_chain = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_filter_push_down_below_limit_by = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_set_shadow;
CREATE TABLE t_set_shadow (k UInt32, v UInt32, w UInt32, INDEX ik k TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
INSERT INTO t_set_shadow SELECT number, number % 10, number % 7 FROM numbers(100);
OPTIMIZE TABLE t_set_shadow FINAL;

-- `k < v AND v < 3` derives `indexHint(k < 3)`, where `k` is `w`
SELECT countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM (SELECT w AS k, v FROM t_set_shadow AS t WHERE t.k < 1000) WHERE k < v AND v < 3);
SELECT count() FROM (SELECT w AS k, v FROM t_set_shadow AS t WHERE t.k < 1000) WHERE k < v AND v < 3;
SELECT count() FROM (SELECT w AS k, v FROM t_set_shadow AS t WHERE t.k < 1000 GROUP BY w, v) WHERE k < v AND v < 3;
SELECT count() FROM (SELECT w AS k, v FROM t_set_shadow AS t WHERE t.k < 1000 ORDER BY w, v LIMIT 1 BY w, v) WHERE k < v AND v < 3;
SELECT count() FROM (SELECT w AS k, v FROM remote('127.0.0.1', currentDatabase(), t_set_shadow) AS t WHERE t.k < 1000) WHERE k < v AND v < 3 SETTINGS prefer_localhost_replica = 1;

-- a hint over `k` itself still prunes
SELECT countIf(explain LIKE '%Granules: 1/25%') FROM (EXPLAIN indexes = 1 SELECT count() FROM (SELECT k AS z, v FROM t_set_shadow AS t WHERE t.k < 1000) WHERE z < v AND v < 3 SETTINGS use_skip_indexes_on_data_read = 0);

DROP TABLE t_set_shadow;

DROP TABLE IF EXISTS t_set_shadow_like;
CREATE TABLE t_set_shadow_like (s String, x String, INDEX is s TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
INSERT INTO t_set_shadow_like SELECT concat('s', toString(number % 5 + 5)), toString(number % 10) FROM numbers(100);
OPTIMIZE TABLE t_set_shadow_like FINAL;

-- the OR-LIKE chain derives a hint over `s`, where `s` is `x`
SELECT countIf(explain LIKE '%multiSearchAny(%') > 0 AND countIf(explain LIKE '%indexHint(%') > 0 FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM (SELECT x AS s FROM t_set_shadow_like AS t WHERE t.s != '') WHERE s LIKE '%1%' OR s LIKE '%2%' OR s LIKE '%3%' OR s LIKE '%4%');
SELECT count() FROM (SELECT x AS s FROM t_set_shadow_like AS t WHERE t.s != '') WHERE s LIKE '%1%' OR s LIKE '%2%' OR s LIKE '%3%' OR s LIKE '%4%';

DROP TABLE t_set_shadow_like;
