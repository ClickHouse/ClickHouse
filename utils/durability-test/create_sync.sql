CREATE TABLE test_sync (a Int, s String) ENGINE = MergeTree ORDER BY a SETTINGS fsync_after_insert = 1, min_compressed_bytes_to_fsync_after_merge = 1;
