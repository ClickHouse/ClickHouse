-- Tags: zookeeper, no-object-storage, no-shared-merge-tree, no-darwin
-- no-object-storage: only parts on local disks are checked with O_DIRECT.
-- no-shared-merge-tree: SharedMergeTree checks parts in its own way.
-- no-darwin: O_DIRECT reads are implemented only on Linux and FreeBSD.

DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_rmt;
DROP TABLE IF EXISTS t_idx_r1;
DROP TABLE IF EXISTS t_idx_r2;

-- Packed parts read their files through the page cache, so the parts have to be full.
CREATE TABLE t_mt (x UInt64, s String) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
CREATE TABLE t_rmt (x UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt', 'r1') ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_mt SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_rmt SELECT number, toString(number) FROM numbers(1000);

CHECK TABLE t_mt SETTINGS check_query_single_value_result = 1, log_comment = 'mt_on';
CHECK TABLE t_rmt SETTINGS check_query_single_value_result = 1, log_comment = 'rmt_on';

-- Skip indices both in skp_idx.packed and in a standalone file, and a column named like a skip index file. A fetched part
-- is loaded without the consistency check, so the index of skp_idx.packed is not read yet (a prewarmed mark cache, or a
-- numeric column in debug builds, reads it).
CREATE TABLE t_idx_r1 (s String, s2 String, skp_idx_value String, INDEX i_minmax s TYPE minmax, INDEX i_bf s2 TYPE bloom_filter)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_idx', 'r1') ORDER BY s
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 1024,
    index_granularity = 8192, index_granularity_bytes = 10485760, replace_long_file_name_to_hash = 0;
CREATE TABLE t_idx_r2 (s String, s2 String, skp_idx_value String, INDEX i_minmax s TYPE minmax, INDEX i_bf s2 TYPE bloom_filter)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_idx', 'r2') ORDER BY s
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, packed_skip_index_max_bytes = 1024,
    index_granularity = 8192, index_granularity_bytes = 10485760, replace_long_file_name_to_hash = 0,
    always_fetch_merged_part = 1, prewarm_mark_cache = 0;
INSERT INTO t_idx_r1 SELECT toString(number), toString(number), toString(number) FROM numbers(100000);
SYSTEM SYNC REPLICA t_idx_r2;
CHECK TABLE t_idx_r2 SETTINGS check_query_single_value_result = 1, log_comment = 'idx_on';

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
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('mt_on', 'rmt_on', 'idx_on', 'mt_off', 'rmt_off')
ORDER BY log_comment;

DROP TABLE t_mt;
DROP TABLE t_rmt;
DROP TABLE t_idx_r1;
DROP TABLE t_idx_r2;
