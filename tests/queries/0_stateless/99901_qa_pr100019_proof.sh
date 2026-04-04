#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS test_metadata_removal_race_1;
DROP TABLE IF EXISTS test_metadata_removal_race_1_new;
DROP TABLE IF EXISTS test_metadata_removal_race_2;
DROP TABLE IF EXISTS test_metadata_removal_race_3;

-- Create tables with local_blob_storage to exercise MetadataStorageFromDisk
CREATE TABLE test_metadata_removal_race_1 (
    id Int64,
    data String
) ENGINE = MergeTree()
ORDER BY id
SETTINGS disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_1/');

CREATE TABLE test_metadata_removal_race_2 (
    id Int64,
    data String
) ENGINE = MergeTree()
ORDER BY id
SETTINGS disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_2/');

CREATE TABLE test_metadata_removal_race_3 (
    id Int64,
    data String
) ENGINE = MergeTree()
ORDER BY id
SETTINGS disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_3/');

-- Insert data to create blobs in parallel
INSERT INTO test_metadata_removal_race_1 SELECT number, toString(number) FROM numbers(100);
INSERT INTO test_metadata_removal_race_2 SELECT number, toString(number) FROM numbers(100);
INSERT INTO test_metadata_removal_race_3 SELECT number, toString(number) FROM numbers(100);

-- Verify data exists
SELECT COUNT(*) FROM test_metadata_removal_race_1;
SELECT COUNT(*) FROM test_metadata_removal_race_2;
SELECT COUNT(*) FROM test_metadata_removal_race_3;
"

# Simulate concurrent removals that might trigger the compaction race
$CLICKHOUSE_CLIENT -q "DROP TABLE test_metadata_removal_race_1 SYNC;" &
$CLICKHOUSE_CLIENT -q "DROP TABLE test_metadata_removal_race_2 SYNC;" &
wait

$CLICKHOUSE_CLIENT -q "
-- Try to create new table with same path after removal to detect zombie entries
CREATE TABLE test_metadata_removal_race_1_new (
    id Int64,
    data String
) ENGINE = MergeTree()
ORDER BY id
SETTINGS disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_1/');

INSERT INTO test_metadata_removal_race_1_new SELECT number, toString(number) FROM numbers(50);
SELECT COUNT(*) FROM test_metadata_removal_race_1_new;

-- Clean up
DROP TABLE IF EXISTS test_metadata_removal_race_1_new SYNC;
DROP TABLE IF EXISTS test_metadata_removal_race_3 SYNC;
"
