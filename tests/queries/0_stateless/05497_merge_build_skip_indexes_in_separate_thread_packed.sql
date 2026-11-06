-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the test needs packed Wide parts with skip index streams written through the part storage.

-- A packed part storage writes all files through one writer, so merges with `merge_build_skip_indexes_in_separate_thread`
-- must not build the skip indexes concurrently with the columns. The skip index streams are not packed into their own
-- archive (`packed_skip_index_max_bytes = 0`), and the buffered data of the part is moved to temporary files.

DROP TABLE IF EXISTS t_skip_thread_packed;

CREATE TABLE t_skip_thread_packed
(
    id UInt64,
    s0 String,
    s1 String,
    INDEX idx_bf s0 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_set s1 TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = '100G', packed_skip_index_max_bytes = 0, max_bytes_to_buffer_for_packed_part = 65536,
    enable_vertical_merge_algorithm = 0, merge_build_skip_indexes_in_separate_thread = 1;

SYSTEM STOP MERGES t_skip_thread_packed;

INSERT INTO t_skip_thread_packed
SELECT number, toString(cityHash64(number, 0) % 1000), toString(cityHash64(number, 1) % 1000)
FROM numbers(100000)
SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 25000, max_insert_block_size = 25000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SYSTEM START MERGES t_skip_thread_packed;
OPTIMIZE TABLE t_skip_thread_packed FINAL;

-- Rewrite the part again with a Vertical merge.
ALTER TABLE t_skip_thread_packed MODIFY SETTING enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
OPTIMIZE TABLE t_skip_thread_packed FINAL;

SELECT part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_skip_thread_packed' AND active;

SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm, ProfileEvents['ExternalPackedPartWriterWritePart'] > 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_skip_thread_packed' AND event_type = 'MergeParts' ORDER BY event_time_microseconds;

SELECT count(), sum(id), groupBitXor(cityHash64(*)) FROM t_skip_thread_packed;
SELECT 'bloom_filter', count() FROM t_skip_thread_packed WHERE s0 = '42' SETTINGS force_data_skipping_indices = 'idx_bf';
SELECT 'set', count() FROM t_skip_thread_packed WHERE s1 = '42' SETTINGS force_data_skipping_indices = 'idx_set';

DROP TABLE t_skip_thread_packed;
