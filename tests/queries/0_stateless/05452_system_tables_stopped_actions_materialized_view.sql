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

-- A view created with a `TO` clause owns no blocker and has no implicit inner table: `SYSTEM STOP
-- MERGES` on it registers nothing, and the destination table it writes to is not touched either.
CREATE TABLE stopped_actions_mv_to_destination (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE stopped_actions_mv_to_source (x UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW stopped_actions_mv_to TO stopped_actions_mv_to_destination AS SELECT x FROM stopped_actions_mv_to_source;
SYSTEM STOP MERGES stopped_actions_mv_to;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_mv_to';
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_mv_to_destination';
DROP TABLE stopped_actions_mv_to SYNC;
DROP TABLE stopped_actions_mv_to_destination SYNC;
DROP TABLE stopped_actions_mv_to_source;

-- A refreshable view keeps its refresh state in `system.view_refreshes`: `SYSTEM STOP VIEW` stops the
-- refresher but registers no table-level action lock, so nothing is reported here.
-- `APPEND` with `all_replicas` is the combination a `Replicated` database accepts for a non-replicated
-- inner engine, so the test runs both there and in an `Atomic` database.
CREATE TABLE stopped_actions_refresh_source (x UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW stopped_actions_refreshable
REFRESH EVERY 1 YEAR SETTINGS all_replicas = 1 APPEND (x UInt64) ENGINE = Memory EMPTY
AS SELECT x FROM stopped_actions_refresh_source;
SYSTEM STOP VIEW stopped_actions_refreshable;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_refreshable';
SYSTEM START VIEW stopped_actions_refreshable;
DROP TABLE stopped_actions_refreshable SYNC;
DROP TABLE stopped_actions_refresh_source;
