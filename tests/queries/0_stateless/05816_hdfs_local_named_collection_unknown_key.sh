#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: HDFS and Iceberg are not in the fast test build

# A key-value argument that the HDFS or Local configuration does not read is rejected in the named collection form.
# Keys stored in the collection itself are not checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_${CLICKHOUSE_TEST_UNIQUE_NAME}"
P="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "
        SET ast_fuzzer_any_query = 0;
        DROP TABLE IF EXISTS t_hdfs; DROP TABLE IF EXISTS t_as; DROP TABLE IF EXISTS t_local;
    " && ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${NC}_hdfs; DROP NAMED COLLECTION IF EXISTS ${NC}_local"
    rm -rf "${P}"*
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "
CREATE NAMED COLLECTION ${NC}_hdfs AS url = 'hdfs://localhost:12222/a.tsv', format = 'TSV', structure = 'x UInt32', stray_key = 'kept';
CREATE NAMED COLLECTION ${NC}_local AS path = '${P}none', format = 'Parquet', structure = 'x Int32', stray_key = 'kept';
"

# Prints only the "Unexpected key" part of the error of a query that must fail.
function expect_unexpected_key()
{
    local key=$1 query=$2
    if output=$(${CLICKHOUSE_CLIENT} -q "SET ast_fuzzer_any_query = 0; $query" 2>&1); then
        echo "Expected a refusal: $query"
    fi
    echo "$output" | grep -o "Unexpected key \`$key\` in named collection" | head -1
}

echo "-- hdfs"
expect_unexpected_key foo_bar "DESCRIBE TABLE hdfs(${NC}_hdfs, foo_bar = 'x')"
expect_unexpected_key formt "DESCRIBE TABLE hdfs(${NC}_hdfs, formt = 'CSV')"
expect_unexpected_key Structure "DESCRIBE TABLE hdfs(${NC}_hdfs, Structure = 'y UInt8')"
expect_unexpected_key foo_bar "DESCRIBE TABLE icebergHDFS(${NC}_hdfs, foo_bar = 'x')"
expect_unexpected_key formt "CREATE TABLE t_hdfs (x UInt32) ENGINE = HDFS(${NC}_hdfs, formt = 'CSV')"
expect_unexpected_key foo_bar "CREATE TABLE t_as AS hdfs(${NC}_hdfs, foo_bar = 'x')"

echo "-- local"
expect_unexpected_key foo_bar "DESCRIBE TABLE icebergLocal(${NC}_local, foo_bar = 'x')"
expect_unexpected_key filename "DESCRIBE TABLE icebergLocal(${NC}_local, filename = 'x')"
expect_unexpected_key foo_bar "CREATE TABLE t_local (x Int32) ENGINE = IcebergLocal(${NC}_local, foo_bar = 'x')"

echo "-- the keys the configurations read are accepted, a key stored in the collection is not checked"
${CLICKHOUSE_CLIENT} -q "DESCRIBE TABLE hdfs(${NC}_hdfs)"
${CLICKHOUSE_CLIENT} -q "DESCRIBE TABLE hdfs(${NC}_hdfs, url = 'hdfs://localhost:12222/b.tsv', filename = 'c.tsv', format = 'CSV', compression_method = 'none', structure = 'y UInt8')"
${CLICKHOUSE_CLIENT} -q "DESCRIBE TABLE hdfs(${NC}_hdfs, compression = 'gzip')"
${CLICKHOUSE_CLIENT} -q "DESCRIBE TABLE icebergLocal(${NC}_local, path = '${P}other', format = 'Parquet', structure = 'y Int32')"
