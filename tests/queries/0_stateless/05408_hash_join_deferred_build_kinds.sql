-- Every kind and strictness gives the result of the streaming build, with both layouts. A build defers only when it
-- holds no more than the streaming build. So with a spill threshold every join defers except those whose map keeps one
-- row per key: `INNER` and `LEFT` `ANY`, `LEFT SEMI` and `LEFT ANTI`. Without one, the stored blocks of an `INNER` or
-- `LEFT` join keep the keys only for `OR` conditions or for a condition on both tables that the join evaluates itself,
-- and only then does it defer. The planner turns such a condition of an `INNER` join into a filter after the join.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 10737418240; -- Far above what these builds take.
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;
DROP TABLE IF EXISTS t_u;

CREATE TABLE t_l (k UInt64, k2 UInt32, s String, t UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_r
(
    k UInt64, k2 UInt32, s String, ns Nullable(String), lc LowCardinality(String), nk Nullable(UInt64), t UInt64,
    keep UInt8, v UInt64
)
ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_u (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();

-- Three rows per key on the right, and half of the left keys miss it.
INSERT INTO t_l SELECT number, number % 7, toString(number), number * 3, number * 13 FROM numbers(40000);
INSERT INTO t_r
SELECT
    number % 20000 AS k, k % 7, toString(k), if(number % 9 = 0, NULL, toString(k)), toString(k % 1000),
    if(number % 10 = 0, NULL, k), number, number % 3 = 0, number * 7
FROM numbers(60000);
-- Unique right keys: the `ALL` join turns into `RightAny`.
INSERT INTO t_u SELECT number, number * 5 FROM numbers(20000);

SELECT 'inner_all', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_all', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_all', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'left_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'left_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'left_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'right_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'right_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'right_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'full_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l FULL JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'full_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l FULL JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'full_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l FULL JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_any', count(), sum(cityHash64(l.k, l.v)) FROM t_l l ANY INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_any', count(), sum(cityHash64(l.k, l.v)) FROM t_l l ANY INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_any', count(), sum(cityHash64(l.k, l.v)) FROM t_l l ANY INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'left_any', count(), sum(cityHash64(l.k, l.v, r.k)) FROM t_l l ANY LEFT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'left_any', count(), sum(cityHash64(l.k, l.v, r.k)) FROM t_l l ANY LEFT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'left_any', count(), sum(cityHash64(l.k, l.v, r.k)) FROM t_l l ANY LEFT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'right_any', count(), sum(cityHash64(l.k, r.k, r.v)) FROM t_l l ANY RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'right_any', count(), sum(cityHash64(l.k, r.k, r.v)) FROM t_l l ANY RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'right_any', count(), sum(cityHash64(l.k, r.k, r.v)) FROM t_l l ANY RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'left_semi', count(), sum(cityHash64(l.k, l.v)) FROM t_l l LEFT SEMI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'left_semi', count(), sum(cityHash64(l.k, l.v)) FROM t_l l LEFT SEMI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'left_semi', count(), sum(cityHash64(l.k, l.v)) FROM t_l l LEFT SEMI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'left_anti', count(), sum(cityHash64(l.k, l.v)) FROM t_l l LEFT ANTI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'left_anti', count(), sum(cityHash64(l.k, l.v)) FROM t_l l LEFT ANTI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'left_anti', count(), sum(cityHash64(l.k, l.v)) FROM t_l l LEFT ANTI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'right_semi', count(), sum(cityHash64(r.k, r.v)) FROM t_l l RIGHT SEMI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'right_semi', count(), sum(cityHash64(r.k, r.v)) FROM t_l l RIGHT SEMI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'right_semi', count(), sum(cityHash64(r.k, r.v)) FROM t_l l RIGHT SEMI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'right_anti', count(), sum(cityHash64(r.k, r.v)) FROM t_l l RIGHT ANTI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'right_anti', count(), sum(cityHash64(r.k, r.v)) FROM t_l l RIGHT ANTI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'right_anti', count(), sum(cityHash64(r.k, r.v)) FROM t_l l RIGHT ANTI JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'asof_inner', count(), sum(cityHash64(l.k, l.t, r.t, r.v)) FROM t_l l ASOF INNER JOIN t_r r ON l.k = r.k AND l.t >= r.t
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'asof_inner', count(), sum(cityHash64(l.k, l.t, r.t, r.v)) FROM t_l l ASOF INNER JOIN t_r r ON l.k = r.k AND l.t >= r.t
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'asof_inner', count(), sum(cityHash64(l.k, l.t, r.t, r.v)) FROM t_l l ASOF INNER JOIN t_r r ON l.k = r.k AND l.t >= r.t
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'asof_left', count(), sum(cityHash64(l.k, l.t, r.t, r.v)) FROM t_l l ASOF LEFT JOIN t_r r ON l.k = r.k AND l.t >= r.t
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'asof_left', count(), sum(cityHash64(l.k, l.t, r.t, r.v)) FROM t_l l ASOF LEFT JOIN t_r r ON l.k = r.k AND l.t >= r.t
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'asof_left', count(), sum(cityHash64(l.k, l.t, r.t, r.v)) FROM t_l l ASOF LEFT JOIN t_r r ON l.k = r.k AND l.t >= r.t
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_all_unique_right', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_u r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_all_unique_right', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_u r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_all_unique_right', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_u r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_mixed_on', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k AND l.v < r.v
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_mixed_on', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k AND l.v < r.v
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_mixed_on', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k AND l.v < r.v
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'left_right_filter', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'left_right_filter', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'left_right_filter', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'right_right_filter', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'right_right_filter', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'right_right_filter', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'left_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'left_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'left_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_string', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_string', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_string', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.s
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_low_cardinality', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_low_cardinality', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_low_cardinality', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'full_nullable', count(), sum(cityHash64(l.k, l.v, r.nk, r.v)) FROM t_l l FULL JOIN t_r r ON l.k = r.nk
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'full_nullable', count(), sum(cityHash64(l.k, l.v, r.nk, r.v)) FROM t_l l FULL JOIN t_r r ON l.k = r.nk
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'full_nullable', count(), sum(cityHash64(l.k, l.v, r.nk, r.v)) FROM t_l l FULL JOIN t_r r ON l.k = r.nk
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_nullable_string', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.ns
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_nullable_string', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.ns
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_nullable_string', count(), sum(cityHash64(l.s, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.s = r.ns
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_two_keys', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k AND l.k2 = r.k2
SETTINGS enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_two_keys', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k AND l.k2 = r.k2
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_two_keys', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k AND l.k2 = r.k2
SETTINGS enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'inner_all_row_store', count(), sum(cityHash64(l.k, l.v, r.s, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'inner_all_row_store', count(), sum(cityHash64(l.k, l.v, r.s, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'inner_all_row_store', count(), sum(cityHash64(l.k, l.v, r.s, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

SELECT 'right_all_row_store', count(), sum(cityHash64(l.k, l.v, r.k, r.s, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_deferred_build = 0, parallel_hash_join_threshold = 0, log_comment = '05408_off';
SELECT 'right_all_row_store', count(), sum(cityHash64(l.k, l.v, r.k, r.s, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 0, log_comment = '05408_on';
SELECT 'right_all_row_store', count(), sum(cityHash64(l.k, l.v, r.k, r.s, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS min_rows_ratio_for_hash_join_row_store = 0, enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 18446744073709551615, log_comment = '05408_on';

-- Without a spill threshold.
SELECT 'inner_all', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';
SELECT 'left_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';
SELECT 'left_right_filter', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k AND r.keep = 1
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';
SELECT 'right_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';
SELECT 'full_all', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l FULL JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';
SELECT 'left_mixed_on', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k AND l.v < r.v
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_off';
SELECT 'left_mixed_on', count(), sum(cityHash64(l.k, l.v, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k AND l.v < r.v
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';
SELECT 'inner_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l INNER JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';
SELECT 'left_disjuncts', count(), sum(cityHash64(l.k, l.v, r.k, r.v)) FROM t_l l LEFT JOIN t_r r ON l.k = r.k OR l.s = r.lc
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, parallel_hash_join_threshold = 0,
    log_comment = '05408_no_threshold';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, count(), countIf(ProfileEvents['HashJoinDeferredBuilds'] = 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select' AND log_comment = '05408_off'
GROUP BY log_comment;
-- Per case: the runs, and the runs that deferred.
SELECT log_comment, extract(query, 'SELECT \'([a-z_]+)\'') AS name, count(), countIf(ProfileEvents['HashJoinDeferredBuilds'] = 1)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment IN ('05408_on', '05408_no_threshold')
GROUP BY log_comment, name
ORDER BY log_comment, name;

DROP TABLE t_l;
DROP TABLE t_r;
DROP TABLE t_u;
