#!/bin/bash
set -e

CH_PATH=${CH_PATH:=clickhouse}

WORK_TREE="$1"

(
  if [ -n "$WORK_TREE" ]; then
    cd "$WORK_TREE" || exit 129
  fi
  $CH_PATH client -mn -q "

select version();

CREATE DATABASE x;
use x;

DROP TABLE IF EXISTS test_map_regression;

-- Create table matching your schema structure
CREATE TABLE test_map_regression
(
    Timestamp DateTime64(9),
    SpanAttributes Map(LowCardinality(String), String),
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
    -- INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = SharedMergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY Timestamp
SETTINGS storage_policy='s3_with_keeper',

replace_long_file_name_to_hash=1,
min_bytes_for_full_part_storage=134217728,
object_serialization_version='v2',
object_shared_data_serialization_version='map',
object_shared_data_serialization_version_for_zero_level_parts='map',
dynamic_serialization_version='v2',
write_marks_for_substreams_in_compact_parts=1,
max_number_of_merges_with_ttl_in_pool=0,
temporary_directories_lifetime=3600,
enforce_index_structure_match_on_partition_manipulation=1,
max_uncompressed_bytes_in_patches=0,
max_parts_in_total=10000,
replicated_deduplication_window=1000,
replicated_deduplication_window_seconds=604800,
max_replicated_logs_to_keep=10000,
wait_for_unique_parts_send_before_shutdown_ms=45000,
shared_merge_tree_use_outdated_parts_compact_format=0,
shared_merge_tree_create_per_replica_metadata_nodes=0,
allow_reduce_blocking_parts_task=1,
vertical_merge_algorithm_min_rows_to_activate=0,
vertical_merge_algorithm_min_bytes_to_activate=134217728,
vertical_merge_algorithm_min_columns_to_activate=0,
vertical_merge_optimize_lightweight_delete=0,
max_postpone_time_for_failed_replicated_fetches_ms=0,
max_postpone_time_for_failed_replicated_merges_ms=0,
max_postpone_time_for_failed_replicated_tasks_ms=0,
allow_part_offset_column_in_projections=0,
enable_block_number_column=0,
allow_summing_columns_in_partition_or_order_key=1,
allow_remote_fs_zero_copy_replication=1,
allow_experimental_reverse_key=0,
compress_marks=1,
compress_primary_key=1,
columns_and_secondary_indices_sizes_lazy_calculation=0,
merge_tree_enable_clear_old_broken_detached=1,
merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds=15552000,


max_parts_in_total=1000000000
;
-- SETTINGS index_granularity = 10;

SET max_partitions_per_insert_block=10000000, max_threads=0;
-- Generate test data (adjust row count to match your data volume)
INSERT INTO test_map_regression
SELECT
    -- 1000 partitions/parts
    now() - toIntervalDay(number % 20000) as Timestamp,
    map(
        'project.id',
        if(number % 100000 = 0, 'dffe5d60-3faa-421a-acf5-0755a1fb0f80', randomPrintableASCII(36))
    ) as SpanAttributes
FROM numbers(1000000);
;" || exit 129


# part_type:                                   Compact
# part_storage_type:                           Packed

  START=$(date +%s)
  for i in {1..4}; do
      $CH_PATH client -q "
use x;
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;
SYSTEM DROP FILESYSTEM CACHE;
SYSTEM DROP QUERY CACHE;
SYSTEM DROP DNS CACHE;
SYSTEM DROP COMPILED EXPRESSION CACHE;
SYSTEM JEMALLOC PURGE;
SYSTEM DROP PAGE CACHE;
SYSTEM DROP MMAP CACHE;

SET max_threads=1, compatibility='24.10';

SELECT count()
FROM test_map_regression
WHERE
 (Timestamp <= (now()))
 AND (SpanAttributes['project.id'] = 'dffe5d60-3faa-421a-acf5-0755a1fb0f80')
SETTINGS log_comment = 'repro_$i', use_query_cache = 0, use_uncompressed_cache = 0,
    filesystem_cache_max_download_size = 0,
    filesystem_cache_segments_batch_size = 0,
    enable_filesystem_cache = 0,
    merge_tree_max_rows_to_use_cache = 0,
    merge_tree_max_bytes_to_use_cache = 0,

    send_logs_level='trace'
   ;
SYSTEM FLUSH LOGS;" 2>&1 | grep idx_span_attr_value # grep logs
  done
  END=$(date +%s)
  echo "Total time: $((END - START)) seconds"


first=$($CH_PATH client -q "SELECT median(query_duration_ms) FROM system.query_log WHERE log_comment = 'repro_1' and type='QueryFinish'")
median=$($CH_PATH client -q "SELECT median(query_duration_ms) FROM system.query_log WHERE log_comment like 'repro_%' and type='QueryFinish'")

echo $first
echo $median

#if (( median > 2000 )); then
#    exit 1
#fi

)
