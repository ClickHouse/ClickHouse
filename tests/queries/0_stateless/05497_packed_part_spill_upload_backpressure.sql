-- Tags: no-parallel, no-fasttest, zookeeper
-- no-parallel: the failpoint is global and other tests insert packed parts with spilling enabled.
-- no-fasttest: needs an S3 disk and failpoints.

-- An insert that writes several packed parts to object storage starts their uploads before waiting for them.
-- When the buffered data of packed parts is moved to temporary files (`max_bytes_to_buffer_for_packed_part`),
-- the archives are read back into the upload buffers, so the insert waits for the earlier uploads before
-- starting the next one while the started archives take at least that many bytes. The failpoint throws
-- when the insert is about to wait, so the insert fails only if it waits instead of starting all the uploads.
-- Each partition is a separate part, and with the limit of one byte every part waits for the previous one.

DROP TABLE IF EXISTS t_packed_backpressure;
DROP TABLE IF EXISTS t_packed_backpressure_replicated;

CREATE TABLE t_packed_backpressure (p UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_full_part_storage = '1T', max_bytes_to_buffer_for_packed_part = 1;

CREATE TABLE t_packed_backpressure_replicated (p UInt64, k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_packed_backpressure_replicated', 'r1') PARTITION BY p ORDER BY k
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_full_part_storage = '1T', max_bytes_to_buffer_for_packed_part = 1;

SET async_insert = 0, max_block_size = 1000, max_insert_threads = 1, max_insert_delayed_streams_for_parallel_write = 1000;

SYSTEM ENABLE FAILPOINT packed_part_finalization_throw_before_waiting_for_earlier_part;
INSERT INTO t_packed_backpressure SELECT number % 4, number, number FROM numbers(40); -- { serverError FAULT_INJECTED }
SYSTEM ENABLE FAILPOINT packed_part_finalization_throw_before_waiting_for_earlier_part;
INSERT INTO t_packed_backpressure_replicated SELECT number % 4, number, number FROM numbers(40); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT packed_part_finalization_throw_before_waiting_for_earlier_part;

-- The started uploads are cancelled with the insert.
SELECT count() FROM t_packed_backpressure;
SELECT count() FROM t_packed_backpressure_replicated;

-- Without the failpoint, the insert waits for each earlier upload and writes all the parts.
INSERT INTO t_packed_backpressure SELECT number % 4, number, number FROM numbers(40);
INSERT INTO t_packed_backpressure_replicated SELECT number % 4, number, number FROM numbers(40);

SELECT p, count(), sum(v) FROM t_packed_backpressure GROUP BY p ORDER BY p;
SELECT p, count(), sum(v) FROM t_packed_backpressure_replicated GROUP BY p ORDER BY p;
SELECT DISTINCT part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_packed_backpressure', 't_packed_backpressure_replicated') AND active;
CHECK TABLE t_packed_backpressure SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_packed_backpressure_replicated SETTINGS check_query_single_value_result = 1;

DROP TABLE t_packed_backpressure;
DROP TABLE t_packed_backpressure_replicated;
