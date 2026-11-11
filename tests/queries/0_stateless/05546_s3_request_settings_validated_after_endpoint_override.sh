#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3

# The endpoint of this test in tests/config/config.d/s3_settings_override.xml sets min_upload_part_size to 5 MiB,
# so s3_max_upload_part_size = 8 MiB is valid there, although it is below the default min_upload_part_size (16 MiB).
# These settings are invalid for any other S3 storage, so the tables live in a Memory database:
# the queries must not read a database disk or a table disk on S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://localhost:11111/test/05546_s3_request_settings_validated_after_endpoint_override/${CLICKHOUSE_DATABASE}"
# The min/max check applies only when the strict part size is 0.
valid="SETTINGS s3_max_upload_part_size = 8388608, s3_strict_upload_part_size = 0"
invalid="SETTINGS s3_max_upload_part_size = 4194304, s3_strict_upload_part_size = 0"
collection="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="${CLICKHOUSE_DATABASE_1}"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $db.t (x UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO $db.t VALUES (1)"

$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('$url/data.tsv', 'test', 'testtest', 'TSV', 'x UInt8') $valid SELECT x FROM $db.t"
$CLICKHOUSE_CLIENT -q "SELECT x FROM s3('$url/data.tsv', 'test', 'testtest', 'TSV', 'x UInt8') $valid"

$CLICKHOUSE_CLIENT -q "BACKUP TABLE $db.t TO S3('$url/backup', 'test', 'testtest') $valid FORMAT Null"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE $db.t AS $db.t_restored FROM S3('$url/backup', 'test', 'testtest') $valid FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT x FROM $db.t_restored"

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS $collection"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION $collection AS url = '$url/collection.tsv', access_key_id = 'test', secret_access_key = 'testtest', format = 'TSV', structure = 'x UInt8'"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3($collection) $valid SELECT x FROM $db.t"
$CLICKHOUSE_CLIENT -q "SELECT x FROM s3($collection) $valid"

# Settings that are invalid after the endpoint settings are applied are still rejected, with the effective values.
error="max_upload_part_size (4.00 MiB) can't be less than setting min_upload_part_size (5.00 MiB)"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3('$url/bad.tsv', 'test', 'testtest', 'TSV', 'x UInt8') $invalid SELECT 1" 2>&1 \
    | grep -o -m1 "$error"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3($collection) $invalid, s3_truncate_on_insert = 1 SELECT 1" 2>&1 \
    | grep -o -m1 "$error"
$CLICKHOUSE_CLIENT -q "SELECT x FROM s3($collection) $invalid" 2>&1 \
    | grep -o -m1 "$error"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE $db.t TO S3('$url/bad_backup', 'test', 'testtest') $invalid FORMAT Null" 2>&1 \
    | grep -o -m1 "$error"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE $db.t AS $db.t_bad FROM S3('$url/backup', 'test', 'testtest') $invalid FORMAT Null" 2>&1 \
    | grep -o -m1 "$error"

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION $collection"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $db"
