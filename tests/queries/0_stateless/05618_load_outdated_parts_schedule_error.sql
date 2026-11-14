-- Tags: no-shared-merge-tree, no-parallel, no-fasttest
-- no-shared-merge-tree: SharedMergeTree doesn't load inactive parts to memory after restart
-- no-parallel: SYSTEM ENABLE FAILPOINT is process-wide

DROP TABLE IF EXISTS t_load_outdated_parts_schedule;

-- Outdated parts must survive DETACH/ATTACH to be loaded in the background after ATTACH.
CREATE TABLE t_load_outdated_parts_schedule (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS old_parts_lifetime = 600;

INSERT INTO t_load_outdated_parts_schedule VALUES (1);
INSERT INTO t_load_outdated_parts_schedule VALUES (2);
INSERT INTO t_load_outdated_parts_schedule VALUES (3);
OPTIMIZE TABLE t_load_outdated_parts_schedule FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_schedule' AND NOT active;

DETACH TABLE t_load_outdated_parts_schedule;

-- No outdated part can be handed to the loading thread pool, as when queries exhaust the global thread pool.
SYSTEM ENABLE FAILPOINT merge_tree_load_outdated_parts_schedule_error;
SYSTEM ENABLE FAILPOINT mt_pause_before_loading_outdated_part;
ATTACH TABLE t_load_outdated_parts_schedule;

-- The first loading attempt is about to hand out a part. Re-arm the pause for the next attempt, then let this one fail.
SYSTEM WAIT FAILPOINT mt_pause_before_loading_outdated_part PAUSE;
SYSTEM ENABLE FAILPOINT mt_pause_before_loading_outdated_part;
SYSTEM NOTIFY FAILPOINT mt_pause_before_loading_outdated_part;

-- The next attempt starts only if the failed one did not terminate the server and scheduled a retry.
SYSTEM WAIT FAILPOINT mt_pause_before_loading_outdated_part PAUSE;

SELECT count() FROM t_load_outdated_parts_schedule;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_schedule' AND NOT active;

SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_schedule_error;
SYSTEM DISABLE FAILPOINT mt_pause_before_loading_outdated_part;
SYSTEM WAIT LOADING PARTS t_load_outdated_parts_schedule;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_schedule' AND NOT active;

DROP TABLE t_load_outdated_parts_schedule;
