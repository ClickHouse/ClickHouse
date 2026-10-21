-- A projection whose SELECT gives an expression the name of a column gets the same key from an ALTER in a session
-- with `prefer_column_name_to_alias = 1` as from a reload of the table, which analyzes it with the server settings.

DROP TABLE IF EXISTS t_add;
DROP TABLE IF EXISTS t_col;
DROP TABLE IF EXISTS t_agg;

CREATE TABLE t_add (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_col (k UInt64, v UInt64, PROJECTION p (SELECT _part_offset, k, v, 100 - v AS v ORDER BY (v, k)))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_agg (k UInt64, v UInt64, PROJECTION pa (SELECT 100 - v AS v, count() GROUP BY v))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;

SET prefer_column_name_to_alias = 1;

SELECT 'ADD PROJECTION';
ALTER TABLE t_add ADD PROJECTION p (SELECT _part_offset, k, v, 100 - v AS v ORDER BY (v, k));
SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_add';
INSERT INTO t_add SELECT number, number % 10 FROM numbers(40);
DETACH TABLE t_add;
ATTACH TABLE t_add;
SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_add';
SELECT count() FROM t_add WHERE 100 - v = 95 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_add WHERE 100 - v = 95 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
INSERT INTO t_add SELECT number + 100, number % 10 FROM numbers(40);
OPTIMIZE TABLE t_add FINAL;
SELECT count() FROM t_add WHERE 100 - v = 95 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_add WHERE 100 - v = 95 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'unrelated ALTER, normal projection';
ALTER TABLE t_col ADD COLUMN x UInt8;
SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_col';
INSERT INTO t_col (k, v) SELECT number, number % 10 FROM numbers(40);
DETACH TABLE t_col;
ATTACH TABLE t_col;
SELECT sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_col';
SELECT count() FROM t_col WHERE 100 - v = 95 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_col WHERE 100 - v = 95 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'unrelated ALTER, aggregate projection';
ALTER TABLE t_agg ADD COLUMN x UInt8;
INSERT INTO t_agg (k, v) SELECT number, number % 10 FROM numbers(40);
DETACH TABLE t_agg;
ATTACH TABLE t_agg;
SELECT groupArray((w, c)) FROM (SELECT 100 - v AS w, count() AS c FROM t_agg GROUP BY w ORDER BY w)
SETTINGS optimize_use_projections = 0;
SELECT groupArray((w, c)) FROM (SELECT 100 - v AS w, count() AS c FROM t_agg GROUP BY w ORDER BY w)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, optimize_aggregation_in_order = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;

DROP TABLE t_add;
DROP TABLE t_col;
DROP TABLE t_agg;
