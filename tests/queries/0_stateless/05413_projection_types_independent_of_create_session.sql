-- The projections of a new table are analyzed without the settings of the session that creates it, the same
-- as when the table is attached: the persisted definition does not record those settings, so the projection
-- index written by `INSERT` must keep its types after a reload.

DROP TABLE IF EXISTS t_05413;

SET enable_extended_results_for_datetime_functions = 1;
CREATE TABLE t_05413
(
    ts DateTime64(3, 'UTC'),
    PROJECTION p (SELECT ts ORDER BY toStartOfDay(ts))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05413 VALUES ('2026-01-01 12:00:00.000');

DETACH TABLE t_05413;
SET enable_extended_results_for_datetime_functions = 0;
ATTACH TABLE t_05413;

SELECT ts FROM t_05413 WHERE toStartOfDay(ts) = toDateTime('2026-01-01 00:00:00', 'UTC')
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

DROP TABLE t_05413;
