-- A projection added or rebuilt by `ALTER` is analyzed without the settings of the session that issues it, the same
-- as when the table is attached: the persisted definition does not record those settings, so the projection index
-- written by `INSERT` must keep its types after a reload.

DROP TABLE IF EXISTS t_05828;

SET enable_extended_results_for_datetime_functions = 1;
CREATE TABLE t_05828 (ts DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_05828 ADD PROJECTION p (SELECT ts ORDER BY toStartOfDay(ts));
INSERT INTO t_05828 VALUES ('2026-01-01 12:00:00.000');

-- An `ALTER` that changes the columns rebuilds the existing projections.
ALTER TABLE t_05828 ADD COLUMN v UInt8;
INSERT INTO t_05828 VALUES ('2026-01-01 13:00:00.000', 1);

-- `MODIFY PROJECTION` rebuilds the projection from its definition.
ALTER TABLE t_05828 MODIFY PROJECTION p (SELECT ts ORDER BY toStartOfDay(ts)) WITH SETTINGS (index_granularity = 8192);
INSERT INTO t_05828 VALUES ('2026-01-01 14:00:00.000', 2);

DETACH TABLE t_05828;
SET enable_extended_results_for_datetime_functions = 0;
ATTACH TABLE t_05828;

SELECT ts FROM t_05828 WHERE toStartOfDay(ts) = toDateTime('2026-01-01 00:00:00', 'UTC') ORDER BY ts
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

DROP TABLE t_05828;
