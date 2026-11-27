#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `icebergS3` table function and the `IcebergS3` engine need the S3 and Avro support.
#
# A data lake table function or engine naming a disk with its `disk` setting is refused for a disk that is not in
# `allowed_disks_for_table_engines` before the disk is looked up or its backend is inspected: the error must not tell
# whether the disk exists or what its backend is (here the local `default` disk, which is not in the list of the test
# configuration, and a disk that does not exist).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

check()
{
    local output
    output=$($CLICKHOUSE_CLIENT -q "$1" 2>&1)
    if grep -q "is not allowed for usage in storage engines" <<< "$output" \
        && ! grep -qE "Unsupported disk type|doesn't match|UNKNOWN_DISK|Local" <<< "$output"
    then
        echo "refused"
    else
        echo "unexpected: $output"
    fi
}

for disk in default "nonexistent_disk_${CLICKHOUSE_DATABASE}"
do
    check "SELECT * FROM icebergS3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/', SETTINGS disk = '${disk}')"
    check "CREATE TABLE t_${CLICKHOUSE_DATABASE} (x Int32) ENGINE = IcebergS3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}/') SETTINGS disk = '${disk}'"
done
