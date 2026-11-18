#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: reads from MinIO
# Tag no-replicated-database: creates a user

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05837_${CLICKHOUSE_DATABASE}_$RANDOM"
s3_path="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}"
file_name="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_s3"
    rm -f "${USER_FILES_PATH:?}/$file_name"
}
trap cleanup EXIT

# `CHECK GRANT` under readonly answers 1 only for the access types readonly permits,
# so an access type added later must not appear below.
privileges=$(${CLICKHOUSE_CLIENT} -q "SELECT privilege FROM system.privileges ORDER BY toString(privilege)")

function check_grant()
{
    paste <(echo "$privileges") \
        <(sed 's/.*/CHECK GRANT & ON *.*;/' <<< "$privileges" | ${CLICKHOUSE_CLIENT} --readonly "$1" --allow_introspection_functions 1) \
        | awk -F'\t' -v result="$2" '$2 == result { print $1 }'
}

echo "-- readonly 0, not granted:"
check_grant 0 0
echo "-- readonly 1, granted:"
check_grant 1 1
echo "-- readonly 2, granted in addition:"
LC_ALL=C comm -13 <(check_grant 1 1) <(check_grant 2 1)

echo "-- INSERT INTO FUNCTION with a filtered WRITE grant, readonly 2:"
${CLICKHOUSE_CLIENT} -q "CREATE USER $user"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE ON *.* TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON URL('http://127.0.0.1:1/.*') TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --readonly 2 -q "INSERT INTO FUNCTION url('http://127.0.0.1:1/x', 'CSV', 'x Int32') SELECT 1" 2>&1 \
    | grep -q -F '(READONLY)' && echo "refused" || echo "not refused"

echo "-- BACKUP, readonly 1:"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t VALUES (1)"
${CLICKHOUSE_CLIENT} --readonly 1 -q "BACKUP TABLE t TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')" | cut -f2

echo "-- file(), readonly 2:"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION file('$file_name', 'CSV', 'x Int32') SELECT 2"
${CLICKHOUSE_CLIENT} --readonly 2 -q "SELECT * FROM file('$file_name', 'CSV', 'x Int32')"

echo "-- S3 database, readonly 2:"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION s3('$s3_path/f.csv', 'clickhouse', 'clickhouse', 'CSV', 'x Int32') SELECT 3 SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_s3 ENGINE = S3('$s3_path/', 'clickhouse', 'clickhouse')"
${CLICKHOUSE_CLIENT} --readonly 2 -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_s3.\`f.csv\`"

echo "-- S3 database, readonly 2, parallel replicas:"
${CLICKHOUSE_CLIENT} --readonly 2 -q "SELECT * FROM ${CLICKHOUSE_DATABASE}_s3.\`f.csv\` SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_cluster_engines = 1, automatic_parallel_replicas_mode = 0"
