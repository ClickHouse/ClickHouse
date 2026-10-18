-- Tags: no-object-storage
-- Tag no-object-storage: fsync is not applicable to object storage

-- With `fsync_after_insert`, an `INSERT` that writes many parts fsyncs the files of all of them before
-- it is acknowledged, even if the parts are merged while the `INSERT` is still running.
-- With `defer_fsync_after_insert` (the default), the fsync is done at the end of the `INSERT`, otherwise after every part.

DROP TABLE IF EXISTS t_fsync_many_parts;
DROP TABLE IF EXISTS t_fsync_many_parts_no_defer;
DROP TABLE IF EXISTS t_fsync_many_parts_replicated;

CREATE TABLE t_fsync_many_parts (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1;

CREATE TABLE t_fsync_many_parts_no_defer (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, defer_fsync_after_insert = 0;

CREATE TABLE t_fsync_many_parts_replicated (k UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fsync_many_parts_replicated', 'r1') ORDER BY k
SETTINGS fsync_after_insert = 1;

INSERT INTO t_fsync_many_parts SELECT number, toString(number) FROM numbers(100000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 4;

INSERT INTO t_fsync_many_parts_no_defer SELECT number, toString(number) FROM numbers(100000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 4;

INSERT INTO t_fsync_many_parts_replicated SELECT number, toString(number) FROM numbers(100000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 4;

SELECT count(), sum(k), sum(toUInt64(s)) FROM t_fsync_many_parts;
SELECT count(), sum(k), sum(toUInt64(s)) FROM t_fsync_many_parts_no_defer;
SELECT count(), sum(k), sum(toUInt64(s)) FROM t_fsync_many_parts_replicated;

SYSTEM FLUSH LOGS query_log, part_log;

SELECT table, count() >= 100 FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND event_type = 'NewPart'
GROUP BY table ORDER BY table;

-- Every file written by the `INSERT` is fsynced.
SELECT
    replaceOne(arrayFirst(x -> startsWith(x, currentDatabase() || '.'), tables), currentDatabase() || '.', '') AS t,
    ProfileEvents['FileSync'] > 0,
    ProfileEvents['FileSync'] = ProfileEvents['FileOpen']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
ORDER BY t;

DROP TABLE t_fsync_many_parts;
DROP TABLE t_fsync_many_parts_no_defer;
DROP TABLE t_fsync_many_parts_replicated;
