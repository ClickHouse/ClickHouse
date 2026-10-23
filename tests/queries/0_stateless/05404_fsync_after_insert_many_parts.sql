-- Tags: no-object-storage
-- Tag no-object-storage: fsync is not applicable to object storage

-- With `fsync_after_insert`, an `INSERT` that writes many parts fsyncs the files of all of them before
-- it is acknowledged, even if the parts are merged while the `INSERT` is still running.
-- With `group_fsync_before_insert_finish` (the default), the fsync is done at the end of the `INSERT`, otherwise after every part.

DROP TABLE IF EXISTS t_fsync_many_parts;
DROP TABLE IF EXISTS t_fsync_many_parts_no_defer;
DROP TABLE IF EXISTS t_fsync_many_parts_replicated;
DROP TABLE IF EXISTS t_fsync_dedup;
DROP TABLE IF EXISTS t_fsync_dedup_replicated;

CREATE TABLE t_fsync_many_parts (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1;

CREATE TABLE t_fsync_many_parts_no_defer (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, group_fsync_before_insert_finish = 0;

CREATE TABLE t_fsync_many_parts_replicated (k UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fsync_many_parts_replicated', 'r1') ORDER BY k
SETTINGS fsync_after_insert = 1;

INSERT INTO t_fsync_many_parts SELECT number, toString(number) FROM numbers(20000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 4;

INSERT INTO t_fsync_many_parts_no_defer SELECT number, toString(number) FROM numbers(20000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 4;

INSERT INTO t_fsync_many_parts_replicated SELECT number, toString(number) FROM numbers(20000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 4;

SELECT count(), sum(k), sum(toUInt64(s)) FROM t_fsync_many_parts;
SELECT count(), sum(k), sum(toUInt64(s)) FROM t_fsync_many_parts_no_defer;
SELECT count(), sum(k), sum(toUInt64(s)) FROM t_fsync_many_parts_replicated;

-- An `INSERT` that is deduplicated relies on the part written by another `INSERT`,
-- that may not be synced yet, so it syncs this part before it is acknowledged.
CREATE TABLE t_fsync_dedup (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, non_replicated_deduplication_window = 100;

CREATE TABLE t_fsync_dedup_replicated (k UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fsync_dedup_replicated', 'r1') ORDER BY k
SETTINGS fsync_after_insert = 1;

INSERT INTO t_fsync_dedup SETTINGS insert_deduplicate = 1, async_insert = 0 VALUES (1), (2), (3);
INSERT INTO t_fsync_dedup SETTINGS insert_deduplicate = 1, async_insert = 0, log_comment = 'deduplicated' VALUES (1), (2), (3);
INSERT INTO t_fsync_dedup_replicated SETTINGS insert_deduplicate = 1, async_insert = 0 VALUES (1), (2), (3);
INSERT INTO t_fsync_dedup_replicated SETTINGS insert_deduplicate = 1, async_insert = 0, log_comment = 'deduplicated' VALUES (1), (2), (3);

SELECT count(), sum(k) FROM t_fsync_dedup;
SELECT count(), sum(k) FROM t_fsync_dedup_replicated;

SYSTEM FLUSH LOGS query_log, part_log;

SELECT table, count() >= 20 FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND event_type = 'NewPart' AND table LIKE 't_fsync_many_parts%'
GROUP BY table ORDER BY table;

-- Every file written by the `INSERT` is fsynced: the deferred fsync syncs as many files as the inline one.
-- (`FileOpen` is not comparable, because debug builds read the written files back to validate them.)
SELECT
    replaceOne(arrayFirst(x -> startsWith(x, currentDatabase() || '.'), tables), currentDatabase() || '.', '') AS t,
    ProfileEvents['FileSync'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND t LIKE 't_fsync_many_parts%'
ORDER BY t;

SELECT uniqExact(ProfileEvents['FileSync'])
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND hasAny(tables, arrayMap(x -> currentDatabase() || '.' || x, ['t_fsync_many_parts', 't_fsync_many_parts_no_defer', 't_fsync_many_parts_replicated']));

-- The deduplicated `INSERT` syncs the existing part.
SELECT
    replaceOne(arrayFirst(x -> startsWith(x, currentDatabase() || '.'), tables), currentDatabase() || '.', '') AS t,
    ProfileEvents['FileSync'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment = 'deduplicated'
ORDER BY t;

DROP TABLE t_fsync_many_parts;
DROP TABLE t_fsync_many_parts_no_defer;
DROP TABLE t_fsync_many_parts_replicated;
DROP TABLE t_fsync_dedup;
DROP TABLE t_fsync_dedup_replicated;
