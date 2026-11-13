-- A `FixedString` compared with another type is compared zero-padded, while its conversion to `String` keeps the padding.

SET cast_fixed_string_to_string_strip_trailing_zeros = 0;

SELECT '-- array order: other tuple fields are compared as in arrays of the same type';
SELECT [tuple(toFixedString('a', 2), nan)] > [tuple('a', 1.)], [tuple('a', nan)] > [tuple('a', 1.)];
SELECT [tuple(toFixedString('a', 2), nan)] <= [tuple('a', nan)], [tuple(toFixedString('a', 2), nan)] = [tuple('a', nan)];
SELECT [tuple(toFixedString('a', 2), CAST(tuple(NULL) AS Dynamic))] <= [tuple('a\0', CAST(tuple(NULL) AS Dynamic))];
SELECT [tuple(toFixedString('a', 2), CAST(tuple(NULL) AS Dynamic))] = [tuple('a', CAST(tuple(NULL) AS Dynamic))];
SELECT
    [tuple(toFixedString('a', 2), CAST(tuple(1) AS Dynamic))] < [tuple('a', CAST(tuple(NULL) AS Dynamic))]
        = ([tuple('a\0', CAST(tuple(1) AS Dynamic))] < [tuple('a\0', CAST(tuple(NULL) AS Dynamic))]),
    [tuple(toFixedString('a', 2), CAST(tuple(1) AS Dynamic))] > [tuple('a', CAST(tuple(NULL) AS Dynamic))]
        = ([tuple('a\0', CAST(tuple(1) AS Dynamic))] > [tuple('a\0', CAST(tuple(NULL) AS Dynamic))]);

SELECT '-- CASE with compound keys';
SELECT
    tuple('a') = tuple(toFixedString('a', 2)),
    CASE tuple('a') WHEN tuple(toFixedString('a', 2)) THEN 1 WHEN tuple('b') THEN 2 ELSE 0 END,
    CASE tuple('a') WHEN materialize(tuple(toFixedString('a', 2))) THEN 1 WHEN tuple('b') THEN 2 ELSE 0 END;
SELECT
    CASE ['a'] WHEN [toFixedString('a', 2)] THEN 1 WHEN ['b'] THEN 2 ELSE 0 END,
    CASE ['a'] WHEN materialize([toFixedString('a', 2)]) THEN 1 WHEN ['b'] THEN 2 ELSE 0 END;

SELECT '-- coalesce in the primary key analysis';
DROP TABLE IF EXISTS t_coalesce_fs;
CREATE TABLE t_coalesce_fs (f Nullable(FixedString(2)), s Nullable(String))
ENGINE = MergeTree ORDER BY f SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_coalesce_fs SELECT 'a', NULL FROM numbers(1000);
INSERT INTO t_coalesce_fs VALUES ('b', NULL);
SELECT count() FROM t_coalesce_fs WHERE coalesce(f, s, 'x') > 'a' SETTINGS use_primary_key = 0;
SELECT count() FROM t_coalesce_fs WHERE coalesce(f, s, 'x') > 'a';
SELECT count() FROM t_coalesce_fs WHERE coalesce(f, s, 'x') > 'a' SETTINGS allow_key_condition_coalesce_rewrite = 0;
DROP TABLE t_coalesce_fs;

SELECT '-- runtime filter index analysis over a LowCardinality(FixedString) key';
DROP TABLE IF EXISTS t_lc_fs;
DROP TABLE IF EXISTS t_lc_s;
CREATE TABLE t_lc_fs (k LowCardinality(FixedString(2))) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE t_lc_s (k String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_lc_fs VALUES ('a'), ('b');
INSERT INTO t_lc_s VALUES ('a'), ('b');
SELECT count() FROM t_lc_fs JOIN t_lc_s USING k
SETTINGS query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0, enable_join_runtime_filters = 1,
    enable_join_runtime_filters_index_analysis = 1, join_runtime_filter_min_probe_rows = 0;
SELECT count() FROM t_lc_fs JOIN t_lc_s USING k
SETTINGS query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0, enable_join_runtime_filters = 1,
    enable_join_runtime_filters_index_analysis = 0, join_runtime_filter_min_probe_rows = 0;
DROP TABLE t_lc_fs;
DROP TABLE t_lc_s;
