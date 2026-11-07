#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# With `s3_create_new_file_on_insert`, the sequence number must be inserted before the first dot
# of the file name, not before the first dot of the whole key (which may be in a directory name).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_DATABASE}/team.v1"

for i in 1 2 3; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${dir}/data.tsv', format = TSV, structure = 'x UInt32') SETTINGS s3_create_new_file_on_insert = 1 VALUES ($i)"
done

# All files must be in the same directory.
$CLICKHOUSE_CLIENT -q "SELECT _file, x FROM s3(s3_conn, filename = '${dir}/*', format = TSV, structure = 'x UInt32') ORDER BY x"

# A file name without an extension.
for i in 1 2; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${dir}/data', format = TSV, structure = 'x UInt32') SETTINGS s3_create_new_file_on_insert = 1 VALUES ($i)"
done
$CLICKHOUSE_CLIENT -q "SELECT _file, x FROM s3(s3_conn, filename = '${dir}/*', format = TSV, structure = 'x UInt32') WHERE _file NOT LIKE '%.tsv' ORDER BY x"

# Partitioned write, where the partition id puts a dot into a directory name.
for _ in 1 2; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION s3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/part.{_partition_id}/data.tsv', format = TSV, structure = 'x UInt32') PARTITION BY x SETTINGS s3_create_new_file_on_insert = 1 VALUES (7)"
done
$CLICKHOUSE_CLIENT -q "SELECT _path, x FROM s3(s3_conn, filename = '${CLICKHOUSE_DATABASE}/part.*/*', format = TSV, structure = 'x UInt32') ORDER BY _path" | sed "s|${CLICKHOUSE_DATABASE}|DB|"
