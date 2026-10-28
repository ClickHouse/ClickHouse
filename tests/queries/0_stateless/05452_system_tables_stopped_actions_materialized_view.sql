-- Tags: memory-engine

CREATE TABLE stopped_actions_mv_source (x UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW stopped_actions_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM stopped_actions_mv_source;

-- The view owns no blocker: `SYSTEM STOP MERGES` on it is forwarded to the implicit inner table,
-- which is where the lock is registered and therefore reported.
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_mv';
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%';

SYSTEM STOP MERGES stopped_actions_mv;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_mv';
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%';

SYSTEM START MERGES stopped_actions_mv;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_mv';
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%';

DROP TABLE stopped_actions_mv SYNC;
DROP TABLE stopped_actions_mv_source;

-- A refreshable view keeps its refresh state in `system.view_refreshes`: `SYSTEM STOP VIEW` stops the
-- refresher but registers no table-level action lock, so nothing is reported here.
CREATE TABLE stopped_actions_refresh_source (x UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW stopped_actions_refreshable REFRESH EVERY 1 YEAR (x UInt64) ENGINE = Memory EMPTY
AS SELECT x FROM stopped_actions_refresh_source;
SYSTEM STOP VIEW stopped_actions_refreshable;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refreshable';
SYSTEM START VIEW stopped_actions_refreshable;
DROP TABLE stopped_actions_refreshable SYNC;
DROP TABLE stopped_actions_refresh_source;
