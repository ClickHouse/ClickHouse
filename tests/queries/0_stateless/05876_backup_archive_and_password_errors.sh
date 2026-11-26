#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The archive rules of the backup locator shared by the backup engines: no password without an archive,
# no archive for `ON CLUSTER`, no zip on `S3`.

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x"

disk="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}"
s3="S3('http://localhost:11111/test/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}"
s3_credentials="'test', 'testtest')"

# Prints the first error starting with $2, with its error code.
function error()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -o -m1 -E "$2[^(]*\([A-Z_]+\)"
}

error "BACKUP TABLE t TO ${disk}') SETTINGS password = 'p'" "Password is not applicable"
error "BACKUP TABLE t TO ${s3}', ${s3_credentials} SETTINGS password = 'p'" "Password is not applicable"
error "BACKUP TABLE t ON CLUSTER test_shard_localhost TO ${disk}_on_cluster.tar')" "Using archives"
error "BACKUP TABLE t ON CLUSTER test_shard_localhost TO ${s3}_on_cluster.tar', ${s3_credentials}" "Using archives"
error "BACKUP TABLE t TO ${s3}.zip', ${s3_credentials}" "Zip archive"
