-- An `ALTER TABLE ... MODIFY REFRESH` that changes only the interval must reschedule the view:
-- the refresh strategy's tree hash has to include the intervals, not only the node type.

DROP TABLE IF EXISTS mv_05407;

CREATE MATERIALIZED VIEW mv_05407 REFRESH EVERY 1 YEAR
ENGINE = Memory EMPTY AS SELECT 1 AS x;

SELECT next_refresh_time > now() + INTERVAL 10 DAY FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05407';

ALTER TABLE mv_05407 MODIFY REFRESH EVERY 1 DAY;

SELECT next_refresh_time <= now() + INTERVAL 2 DAY FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05407';

DROP TABLE mv_05407;
