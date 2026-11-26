-- `-0.` and `0.` are the same key in `IN`, `GROUP BY`, `DISTINCT`, `LIMIT BY` and hash `JOIN`.
-- Check the alternate paths that hash the keys: the Bloom runtime filter of a join, the `bloom_filter` skip index,
-- the aggregation in order of a prefix of the keys, and the spilled `DISTINCT`.

SELECT '-- Bloom runtime filter';
DROP TABLE IF EXISTS t_nz_probe;
DROP TABLE IF EXISTS t_nz_build;
CREATE TABLE t_nz_probe (t UInt32, k Float64) ENGINE = MergeTree ORDER BY t;
CREATE TABLE t_nz_build (k Float64, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nz_probe VALUES (0, 0.), (1, -0.), (2, 1.5);
INSERT INTO t_nz_build VALUES (0., 'zero'), (1.5, 'p');
INSERT INTO t_nz_build SELECT 1000000 + number, 'x' FROM numbers(1000);

SELECT count() FROM t_nz_probe INNER JOIN t_nz_build USING (k)
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 0, join_algorithm = 'hash', query_plan_join_swap_table = 0;
SELECT count() FROM t_nz_probe INNER JOIN t_nz_build USING (k)
SETTINGS enable_join_runtime_filters = 0, join_algorithm = 'hash', query_plan_join_swap_table = 0;

-- Only `-0.` on the build side.
TRUNCATE TABLE t_nz_build;
INSERT INTO t_nz_build VALUES (-0., 'neg zero'), (1.5, 'p');
INSERT INTO t_nz_build SELECT 1000000 + number, 'x' FROM numbers(1000);

SELECT count() FROM t_nz_probe INNER JOIN t_nz_build USING (k)
SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_exact_values_limit = 0, join_algorithm = 'hash', query_plan_join_swap_table = 0;
SELECT count() FROM t_nz_probe INNER JOIN t_nz_build USING (k)
SETTINGS enable_join_runtime_filters = 0, join_algorithm = 'hash', query_plan_join_swap_table = 0;

DROP TABLE t_nz_probe;
DROP TABLE t_nz_build;

SELECT '-- bloom_filter skip index';
DROP TABLE IF EXISTS t_nz_index;
CREATE TABLE t_nz_index (id UInt32, f Float64, a Array(Float64), INDEX idx_f f TYPE bloom_filter GRANULARITY 1, INDEX idx_a a TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_nz_index VALUES (1, -0., [-0.]), (2, 2., [2.]), (3, 3., [3.]);

SELECT count() FROM t_nz_index WHERE f IN (0., 1.) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_nz_index WHERE f IN (0., 1.) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_nz_index WHERE (id, f) IN ((1, 0.), (5, 1.)) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_nz_index WHERE (id, f) IN ((1, 0.), (5, 1.)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_nz_index WHERE a IN ([0.], [1.]) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_nz_index WHERE a IN ([0.], [1.]) SETTINGS use_skip_indexes = 0;
-- Without a zero in the set the index is still used.
SELECT count() FROM t_nz_index WHERE f IN (2., 5.) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_f';

DROP TABLE t_nz_index;

SELECT '-- Aggregation in order of a prefix of the keys';
DROP TABLE IF EXISTS t_nz_in_order;
CREATE TABLE t_nz_in_order (k UInt32, f Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_nz_in_order VALUES (1, 0.), (1, -0.), (1, 2.), (2, -0.), (2, 0.);

SELECT count() FROM (SELECT k, f FROM t_nz_in_order GROUP BY k, f) SETTINGS optimize_aggregation_in_order = 0, max_threads = 1;
SELECT count() FROM (SELECT k, f FROM t_nz_in_order GROUP BY k, f) SETTINGS optimize_aggregation_in_order = 1, max_threads = 1;

DROP TABLE t_nz_in_order;

SELECT '-- Spilled DISTINCT of a generic key';
SELECT count() FROM
(
    SELECT DISTINCT if(number % 2 = 0, [0.], [-0.]) AS k FROM numbers(10)
)
SETTINGS max_threads = 1, max_block_size = 2, optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0,
    max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 1;
SELECT count() FROM
(
    SELECT DISTINCT if(number % 2 = 0, [0.], [-0.]) AS k FROM numbers(10)
)
SETTINGS max_threads = 1, max_block_size = 2, optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0,
    max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 0;

SELECT '-- Replicated columns';
SELECT count() FROM
(
    SELECT a FROM (SELECT if(number = 0, [-0.], [0.]) AS a, [1, 2] AS arr FROM numbers(2)) ARRAY JOIN arr GROUP BY a
)
SETTINGS enable_lazy_columns_replication = 1;
SELECT count() FROM
(
    SELECT DISTINCT a FROM (SELECT if(number = 0, [-0.], [0.]) AS a, [1, 2] AS arr FROM numbers(2)) ARRAY JOIN arr
)
SETTINGS enable_lazy_columns_replication = 1;

SELECT '-- Nested columns';
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin([0., -0.])::Variant(Float64, String) AS x) GROUP BY x)
SETTINGS allow_suspicious_types_in_group_by = 1;
SELECT count() FROM (SELECT DISTINCT x FROM (SELECT [arrayJoin([0., -0.])]::Array(LowCardinality(Float64)) AS x))
SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT count() FROM (SELECT x FROM (SELECT ('a', [toNullable(arrayJoin([0., -0.]))]) AS x) GROUP BY x);
SELECT count() FROM (SELECT x FROM (SELECT map('k', arrayJoin([0., -0.])) AS x) GROUP BY x);

-- `Dynamic` keeps a value typed or serialized in the shared variant, depending on the order of inserts. It is not
-- modified, so the same `-0.` has the same key in both layouts.
SELECT '-- Dynamic in both layouts';
DROP TABLE IF EXISTS nz_dynamic;
CREATE TABLE nz_dynamic (d Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO nz_dynamic VALUES (-0.0);
INSERT INTO nz_dynamic VALUES ('seed'), (-0.0);
-- Two groups: 'seed' and `-0.`.
SELECT count() FROM (SELECT d FROM nz_dynamic GROUP BY d) SETTINGS allow_suspicious_types_in_group_by = 1, max_threads = 1;
SELECT count() FROM (SELECT DISTINCT d FROM nz_dynamic) SETTINGS max_threads = 1;
DROP TABLE nz_dynamic;

SELECT '-- A typed path of JSON';
SELECT count() FROM (SELECT j.a AS a FROM (SELECT ('{"a" : ' || arrayJoin(['0.0', '-0.0']) || '}')::JSON(a Float64) AS j) GROUP BY a);
