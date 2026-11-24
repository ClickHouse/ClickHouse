-- `SYSTEM STOP VIEW` and `SYSTEM PAUSE VIEW` through an `Alias` of a refreshable materialized view with an
-- inner table stop the refresher of the view. `SYSTEM START VIEW` through the same `Alias` has to resume it:
-- the action belongs to the view rather than to its inner table, which holds no lock for it.

DROP TABLE IF EXISTS a_mv_05839;
DROP TABLE IF EXISTS mv_05839;

CREATE MATERIALIZED VIEW mv_05839 REFRESH EVERY 1 YEAR (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 1 AS x;
CREATE TABLE a_mv_05839 ENGINE = Alias(currentDatabase(), 'mv_05839');

-- Stopping interrupts a running refresh asynchronously, so let the initial one finish first.
SYSTEM WAIT VIEW mv_05839;
SYSTEM STOP VIEW a_mv_05839;
SELECT 'stopped through the alias', status FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05839';
SYSTEM START VIEW a_mv_05839;
SELECT 'started through the alias', status != 'Disabled' FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05839';

SYSTEM PAUSE VIEW a_mv_05839;
SELECT 'paused through the alias', status FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05839';
SYSTEM START VIEW a_mv_05839;
SELECT 'started through the alias', status != 'Disabled' FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05839';

-- The view named directly, for comparison.
SYSTEM STOP VIEW mv_05839;
SELECT 'stopped directly', status FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05839';
SYSTEM START VIEW mv_05839;
SELECT 'started directly', status != 'Disabled' FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05839';

DROP TABLE a_mv_05839;
DROP TABLE mv_05839;
