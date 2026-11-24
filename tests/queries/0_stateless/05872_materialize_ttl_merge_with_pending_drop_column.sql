-- Tags: no-parallel
-- Tag no-parallel: uses the server-wide failpoint `mt_mutate_task_pause_in_prepare`

-- A merge applies a pending `DROP COLUMN`: the reader ignores the values the source parts store under the
-- dropped name and fills the column added again under that name from its DEFAULT. The merged part then
-- stores other values than the ones its rows-TTL bounds were computed from, so the merge must drop the
-- rows-TTL fingerprint. Otherwise a later `MODIFY TTL` to a shift of the old expression would shift the
-- stale bounds instead of rewriting the part, and remove rows that expire far in the future.

DROP TABLE IF EXISTS t_ttl_hold_mutation;
DROP TABLE IF EXISTS t_ttl_merge_pending_drop;

-- A mutation of another table is paused in the background pool, so that `max_number_of_mutations_for_replica = 1`
-- keeps the `DROP COLUMN` mutation of the table under test pending while its parts are merged.
CREATE TABLE t_ttl_hold_mutation (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_hold_mutation VALUES (1);
SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_in_prepare;
ALTER TABLE t_ttl_hold_mutation UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
SYSTEM WAIT FAILPOINT mt_mutate_task_pause_in_prepare PAUSE;

CREATE TABLE t_ttl_merge_pending_drop
(
    id UInt64,
    d DateTime('UTC') DEFAULT toDateTime('2100-01-01 00:00:00', 'UTC')
)
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, min_bytes_for_wide_part = 0,
    materialize_ttl_recalculate_only = 0, max_number_of_mutations_for_replica = 1;

SYSTEM STOP MERGES t_ttl_merge_pending_drop;
INSERT INTO t_ttl_merge_pending_drop SELECT 1, now('UTC') - INTERVAL 100 DAY;
INSERT INTO t_ttl_merge_pending_drop SELECT 2, now('UTC') - INTERVAL 100 DAY;

ALTER TABLE t_ttl_merge_pending_drop REMOVE TTL SETTINGS alter_sync = 0;
ALTER TABLE t_ttl_merge_pending_drop DROP COLUMN d SETTINGS alter_sync = 0;
ALTER TABLE t_ttl_merge_pending_drop ADD COLUMN d DateTime('UTC') DEFAULT toDateTime('2100-01-01 00:00:00', 'UTC') SETTINGS alter_sync = 0;

-- The drop is pending when the merge starts.
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_ttl_merge_pending_drop' AND command LIKE '%DROP COLUMN%' AND NOT is_done;

SYSTEM START MERGES t_ttl_merge_pending_drop;
OPTIMIZE TABLE t_ttl_merge_pending_drop FINAL;

-- The merged part stores the new default.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_merge_pending_drop' AND active;
SELECT id, d FROM t_ttl_merge_pending_drop ORDER BY id;

SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare;

ALTER TABLE t_ttl_merge_pending_drop MODIFY TTL d + INTERVAL 10 DAY SETTINGS alter_sync = 2, mutations_sync = 2;
SELECT id, d FROM t_ttl_merge_pending_drop ORDER BY id;

DROP TABLE t_ttl_merge_pending_drop;
DROP TABLE t_ttl_hold_mutation;
