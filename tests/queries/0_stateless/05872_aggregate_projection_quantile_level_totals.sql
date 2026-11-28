-- An aggregate projection storing a state of another quantile level must not change the level of the query
-- when the merged states are finalized by `WITH TOTALS`, `ROLLUP`, `CUBE` or in-order aggregation.

DROP TABLE IF EXISTS t_proj_quantile_level;

CREATE TABLE t_proj_quantile_level (k UInt8, x UInt64, PROJECTION p (SELECT k, quantile(0.3)(x) GROUP BY k)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_proj_quantile_level SELECT number % 2, number FROM numbers(100);

SET optimize_use_projections = 1, force_optimize_projection = 1;

SELECT k, quantile(0.5)(x) FROM t_proj_quantile_level GROUP BY k ORDER BY k;
SELECT k, quantile(0.5)(x) FROM t_proj_quantile_level GROUP BY k WITH TOTALS ORDER BY k;
SELECT k, quantile(0.5)(x) AS q FROM t_proj_quantile_level GROUP BY ROLLUP(k) ORDER BY k, q;
SELECT k, quantile(0.5)(x) AS q FROM t_proj_quantile_level GROUP BY CUBE(k) ORDER BY k, q;
SELECT k, quantile(0.5)(x) FROM t_proj_quantile_level GROUP BY k ORDER BY k SETTINGS optimize_aggregation_in_order = 1;
SELECT k, quantile(0.3)(x), quantile(0.5)(x) FROM t_proj_quantile_level GROUP BY k WITH TOTALS ORDER BY k;

DROP TABLE t_proj_quantile_level;
