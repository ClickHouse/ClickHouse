-- A JOIN runtime filter must not reject a probe key that the hash join matches: `-0.` matches `0.`,
-- also inside composite keys, in both the exact set and the Bloom filter phase of the filter.

SET enable_analyzer = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_limit = 1;
SET join_runtime_filter_min_probe_rows = 0;

DROP TABLE IF EXISTS rf_probe;
DROP TABLE IF EXISTS rf_build;

CREATE TABLE rf_probe (k Float64, a Array(Float64), t Tuple(Float64, String), lc LowCardinality(Float64)) ENGINE = Memory;
CREATE TABLE rf_build (k Float64, a Array(Float64), t Tuple(Float64, String), lc LowCardinality(Float64)) ENGINE = Memory;

INSERT INTO rf_probe VALUES (-0., [-0.], (-0., 'x'), -0.);
INSERT INTO rf_build VALUES (0., [0.], (0., 'x'), 0.), (1., [1.], (1., 'x'), 1.);

SELECT '-- Bloom filter';
SET join_runtime_filter_exact_values_limit = 1;

SELECT 'k', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0;
SELECT 'k', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 1;
SELECT 'a', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.a = b.a SETTINGS enable_join_runtime_filters = 1;
SELECT 't', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.t = b.t SETTINGS enable_join_runtime_filters = 1;
SELECT 'lc', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.lc = b.lc SETTINGS enable_join_runtime_filters = 1;
SELECT 'k anti', count() FROM rf_probe AS p LEFT ANTI JOIN rf_build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 1;

SELECT '-- Exact set';
SET join_runtime_filter_exact_values_limit = 10000;

SELECT 'k', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 1;
SELECT 'a', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.a = b.a SETTINGS enable_join_runtime_filters = 1;
SELECT 't', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.t = b.t SETTINGS enable_join_runtime_filters = 1;
SELECT 'lc', count() FROM rf_probe AS p INNER JOIN rf_build AS b ON p.lc = b.lc SETTINGS enable_join_runtime_filters = 1;
SELECT 'k anti', count() FROM rf_probe AS p LEFT ANTI JOIN rf_build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 1;

SELECT '-- Negative zero on the build side';
SET join_runtime_filter_exact_values_limit = 1;

SELECT 'k', count() FROM rf_build AS p INNER JOIN rf_probe AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 1;
SELECT 'a', count() FROM rf_build AS p INNER JOIN rf_probe AS b ON p.a = b.a SETTINGS enable_join_runtime_filters = 1;

DROP TABLE rf_probe;
DROP TABLE rf_build;
