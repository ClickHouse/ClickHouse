-- Tags: zookeeper

-- With `ttl_only_drop_parts`, `MATERIALIZE TTL` replaces a part whose rows have all expired with an empty part.
-- On a replicated table the empty part must have the name from its `MUTATE_PART` entry. Otherwise the replication
-- queue does not know the part, and every later mutation waits forever for the part named in the entry.
-- Compact and Wide parts are mutated by different tasks, so the test covers both.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_compact SYNC;
DROP TABLE IF EXISTS t_wide SYNC;

CREATE TABLE t_compact (d DateTime('UTC'), p UInt8, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_compact', 'r1')
PARTITION BY p ORDER BY d
SETTINGS ttl_only_drop_parts = 1, materialize_ttl_recalculate_only = 0,
    min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;

CREATE TABLE t_wide (d DateTime('UTC'), p UInt8, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_wide', 'r1')
PARTITION BY p ORDER BY d
SETTINGS ttl_only_drop_parts = 1, materialize_ttl_recalculate_only = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- Partition 0 expires as a whole, partition 1 does not expire.
INSERT INTO t_compact VALUES ('2000-01-01 00:00:00', 0, 1), ('2000-01-01 00:00:00', 0, 2), ('2100-01-01 00:00:00', 1, 3), ('2100-01-01 00:00:00', 1, 4);
INSERT INTO t_wide VALUES ('2000-01-01 00:00:00', 0, 1), ('2000-01-01 00:00:00', 0, 2), ('2100-01-01 00:00:00', 1, 3), ('2100-01-01 00:00:00', 1, 4);

ALTER TABLE t_compact MODIFY TTL d + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_wide MODIFY TTL d + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;

ALTER TABLE t_compact MATERIALIZE TTL;
ALTER TABLE t_wide MATERIALIZE TTL;

SELECT 'compact, after MATERIALIZE TTL', p, count() FROM t_compact GROUP BY p ORDER BY p;
SELECT 'wide, after MATERIALIZE TTL', p, count() FROM t_wide GROUP BY p ORDER BY p;

-- Bound the wait, so that a mutation stuck behind a part that does not exist fails the test instead of hanging it.
ALTER TABLE t_compact UPDATE v = v + 10 WHERE 1 SETTINGS max_execution_time = 120;
ALTER TABLE t_wide UPDATE v = v + 10 WHERE 1 SETTINGS max_execution_time = 120;

SELECT 'compact, after UPDATE', p, sum(v) FROM t_compact GROUP BY p ORDER BY p;
SELECT 'wide, after UPDATE', p, sum(v) FROM t_wide GROUP BY p ORDER BY p;
SELECT 'unfinished mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table IN ('t_compact', 't_wide') AND NOT is_done;

DROP TABLE t_compact SYNC;
DROP TABLE t_wide SYNC;
