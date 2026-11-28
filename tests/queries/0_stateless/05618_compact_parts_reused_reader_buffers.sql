-- Tags: no-random-settings, no-random-merge-tree-settings

-- A reader of a Compact part is reused for the later read tasks of the same part, and reading in order starts with
-- small tasks. The choice between one buffer for all columns and a buffer per column must be made for the ranges of
-- every task, not only for the first one: otherwise a large read with one buffer reads the data of the column `s`,
-- which is not requested. The first stripe holds one granule of big rows, so a read that starts there begins with a
-- part of the file that is not striped, and the later multi-granule stripes must still be read with a buffer per column.

DROP TABLE IF EXISTS t_compact_reused_reader;

CREATE TABLE t_compact_reused_reader (id UInt64, s String, b UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, merge_max_block_size = 2,
    compact_parts_max_granules_to_buffer = 128, compact_parts_max_bytes_to_buffer = 65536;

SYSTEM STOP MERGES t_compact_reused_reader;
INSERT INTO t_compact_reused_reader SELECT number, randomString(100000), number FROM numbers(2);
INSERT INTO t_compact_reused_reader SELECT number + 2, randomString(8192), number + 2 FROM numbers(1002);
SYSTEM START MERGES t_compact_reused_reader;
OPTIMIZE TABLE t_compact_reused_reader FINAL;

SELECT count(), uniqExact(_part) FROM t_compact_reused_reader;

SET max_threads = 1, enable_parallel_replicas = 0, optimize_read_in_order = 1, query_plan_optimize_lazy_materialization = 0,
    optimize_move_to_prewhere = 0, use_uncompressed_cache = 0, use_query_condition_cache = 0, local_filesystem_read_method = 'pread',
    use_page_cache_for_disks_without_file_cache = 0;

SELECT sum(b) FROM (SELECT b FROM t_compact_reused_reader WHERE id >= 32 ORDER BY id LIMIT 900)
SETTINGS log_comment = '05618_multi_granule_stripes';
SELECT sum(b) FROM (SELECT b FROM t_compact_reused_reader WHERE id = 0 OR id >= 32 ORDER BY id LIMIT 900 OFFSET 1)
SETTINGS log_comment = '05618_isolated_stripe_first';

SYSTEM FLUSH LOGS query_log;

-- The data of the column `s` is about 8 MiB. It must not be read.
SELECT log_comment, ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] < 2000000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('05618_multi_granule_stripes', '05618_isolated_stripe_first')
ORDER BY log_comment;

DROP TABLE t_compact_reused_reader;
