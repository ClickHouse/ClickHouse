-- Tags: zookeeper, no-object-storage, no-shared-merge-tree
-- no-object-storage: only parts on local disks are checked with O_DIRECT.
-- no-shared-merge-tree: SharedMergeTree checks parts in its own way.

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_rmt;

-- Packed parts read their files through the page cache, so the parts have to be full.
CREATE TABLE t_mt (x UInt64, s String) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
CREATE TABLE t_rmt (x UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt', 'r1') ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_mt SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_rmt SELECT number, toString(number) FROM numbers(1000);

CHECK TABLE t_mt SETTINGS check_query_single_value_result = 1, log_comment = 'mt_on';
CHECK TABLE t_rmt SETTINGS check_query_single_value_result = 1, log_comment = 'rmt_on';

ALTER TABLE t_mt MODIFY SETTING check_part_use_direct_io = 0;
ALTER TABLE t_rmt MODIFY SETTING check_part_use_direct_io = 0;

CHECK TABLE t_mt SETTINGS check_query_single_value_result = 1, log_comment = 'mt_off';
CHECK TABLE t_rmt SETTINGS check_query_single_value_result = 1, log_comment = 'rmt_off';

SYSTEM FLUSH LOGS query_log;

-- Every file of the part is opened with O_DIRECT (or falls back if the filesystem does not support it).
SELECT
    log_comment,
    ProfileEvents['CreatedReadBufferDirectIO'] + ProfileEvents['CreatedReadBufferDirectIOFailed'] > 0
        AND ProfileEvents['CreatedReadBufferOrdinary'] = ProfileEvents['CreatedReadBufferDirectIOFailed'] AS direct_io,
    ProfileEvents['CreatedReadBufferDirectIO'] = 0 AND ProfileEvents['CreatedReadBufferDirectIOFailed'] = 0
        AND ProfileEvents['CreatedReadBufferOrdinary'] > 0 AS buffered
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('mt_on', 'rmt_on', 'mt_off', 'rmt_off')
ORDER BY log_comment;

DROP TABLE t_mt;
DROP TABLE t_rmt;
