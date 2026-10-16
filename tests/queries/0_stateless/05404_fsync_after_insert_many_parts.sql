-- Tags: no-object-storage
-- Tag no-object-storage: fsync is not applicable to object storage

-- With `fsync_after_insert`, an `INSERT` that writes many parts fsyncs the files of all of them before
-- it is acknowledged, even if the parts are merged while the `INSERT` is still running.

DROP TABLE IF EXISTS t_fsync_many_parts;

CREATE TABLE t_fsync_many_parts (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1;

INSERT INTO t_fsync_many_parts SELECT number, toString(number) FROM numbers(100000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 4;

SELECT count(), sum(k), sum(toUInt64(s)) FROM t_fsync_many_parts;

SYSTEM FLUSH LOGS query_log, part_log;

SELECT count() >= 100 FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 't_fsync_many_parts' AND event_type = 'NewPart';

-- Every file written by the `INSERT` is fsynced.
SELECT ProfileEvents['FileSync'] > 0, ProfileEvents['FileSync'] = ProfileEvents['FileOpen']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND query_kind = 'Insert' AND has(tables, currentDatabase() || '.t_fsync_many_parts');

DROP TABLE t_fsync_many_parts;
