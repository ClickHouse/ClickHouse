-- Tags: atomic-database

SET session_timezone = 'Etc/UTC';

CREATE TABLE refresh_progress_dst (x UInt8) ENGINE = Memory;
-- Give EMPTY a delayed first refresh so the view cannot refresh while setting up the fake clock.
CREATE MATERIALIZED VIEW refresh_progress_mv
REFRESH AFTER 1 DAY
APPEND TO refresh_progress_dst EMPTY AS SELECT toUInt8(1) AS x;

SYSTEM STOP VIEW refresh_progress_mv;
ALTER TABLE refresh_progress_mv MODIFY REFRESH EVERY 1 MINUTE RANDOMIZE FOR 8 SECOND APPEND;
SYSTEM TEST VIEW refresh_progress_mv SET FAKE TIME '2050-01-01 00:01:05';
SYSTEM REFRESH VIEW refresh_progress_mv;
SYSTEM START VIEW refresh_progress_mv;
SYSTEM WAIT VIEW refresh_progress_mv;
SELECT count() FROM refresh_progress_dst;

-- Recreate the state of a scheduled refresh that completed ahead of its timeslot:
-- the 00:01 slot is complete while the clock is still before 00:01.
SYSTEM TEST VIEW refresh_progress_mv SET FAKE TIME '2050-01-01 00:00:55';
SYSTEM REFRESH VIEW refresh_progress_mv;
SYSTEM WAIT VIEW refresh_progress_mv;
SELECT count() FROM refresh_progress_dst;
SELECT dateDiff('second', last_success_time, next_refresh_time) BETWEEN 61 AND 69
FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'refresh_progress_mv';

-- Passing the completed slot must not execute it again.
SYSTEM STOP VIEW refresh_progress_mv;
SYSTEM TEST VIEW refresh_progress_mv SET FAKE TIME '2050-01-01 00:01:05';
SYSTEM START VIEW refresh_progress_mv;
SYSTEM WAIT VIEW refresh_progress_mv;
SELECT count() FROM refresh_progress_dst;

-- A later manual refresh must still advance the completed timeslot.
SYSTEM STOP VIEW refresh_progress_mv;
SYSTEM TEST VIEW refresh_progress_mv SET FAKE TIME '2050-01-01 00:03:15';
SYSTEM REFRESH VIEW refresh_progress_mv;
SYSTEM START VIEW refresh_progress_mv;
SYSTEM WAIT VIEW refresh_progress_mv;
SELECT count() FROM refresh_progress_dst;
SELECT dateDiff('second', last_success_time, next_refresh_time) BETWEEN 41 AND 49
FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'refresh_progress_mv';

-- Preserve normal progress after changing the refresh period.
SYSTEM STOP VIEW refresh_progress_mv;
ALTER TABLE refresh_progress_mv MODIFY REFRESH EVERY 2 MINUTE RANDOMIZE FOR 8 SECOND APPEND;
SYSTEM TEST VIEW refresh_progress_mv SET FAKE TIME '2050-01-01 00:04:15';
SYSTEM REFRESH VIEW refresh_progress_mv;
SYSTEM START VIEW refresh_progress_mv;
SYSTEM WAIT VIEW refresh_progress_mv;
SELECT count() FROM refresh_progress_dst;
SELECT dateDiff('second', last_success_time, next_refresh_time) BETWEEN 101 AND 109
FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'refresh_progress_mv';

-- The completion-based schedule keeps its own behavior.
SYSTEM STOP VIEW refresh_progress_mv;
ALTER TABLE refresh_progress_mv MODIFY REFRESH AFTER 20 SECOND APPEND;
SYSTEM TEST VIEW refresh_progress_mv SET FAKE TIME '2050-01-01 00:05:15';
SYSTEM REFRESH VIEW refresh_progress_mv;
SYSTEM START VIEW refresh_progress_mv;
SYSTEM WAIT VIEW refresh_progress_mv;
SELECT count() FROM refresh_progress_dst;
SELECT dateDiff('second', last_success_time, next_refresh_time) = 20
FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'refresh_progress_mv';

DROP TABLE refresh_progress_mv;
DROP TABLE refresh_progress_dst;
