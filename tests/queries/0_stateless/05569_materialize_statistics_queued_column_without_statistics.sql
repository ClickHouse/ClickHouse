-- A `MATERIALIZE STATISTICS` mutation that was accepted while the column had statistics must not block the
-- mutation queue if the column has no statistics when the mutation is executed. This happens after an upgrade
-- for a mutation queued for a column with explicitly declared statistics (they are ignored now), and when
-- `auto_statistics_types` is changed while the mutation is queued.

DROP TABLE IF EXISTS t_queued_materialize_statistics;

CREATE TABLE t_queued_materialize_statistics (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'tdigest', min_bytes_for_wide_part = 0;

INSERT INTO t_queued_materialize_statistics SELECT number, number FROM numbers(100);

SYSTEM STOP MERGES t_queued_materialize_statistics;
ALTER TABLE t_queued_materialize_statistics MATERIALIZE STATISTICS a SETTINGS mutations_sync = 0;
ALTER TABLE t_queued_materialize_statistics MODIFY SETTING auto_statistics_types = '';

-- A new submission is still validated.
ALTER TABLE t_queued_materialize_statistics MATERIALIZE STATISTICS a; -- { serverError ILLEGAL_STATISTICS }

SYSTEM START MERGES t_queued_materialize_statistics;
ALTER TABLE t_queued_materialize_statistics UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT sum(a), sum(b) FROM t_queued_materialize_statistics;
SELECT count(), countIf(is_done), countIf(latest_fail_reason != '') FROM system.mutations
WHERE database = currentDatabase() AND table = 't_queued_materialize_statistics';

DROP TABLE t_queued_materialize_statistics;
