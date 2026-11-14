#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: needs Azurite
# - no-replicated-database: the replicated access storage widens source grants

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Inferring the structure of `azureBlobStorage` is a read, so it must not create a missing container,
# which needs `WRITE ON AZURE`.

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
AZURE_CONT_PREFIX="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

user="${CLICKHOUSE_DATABASE}_user_05565"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TEMPORARY TABLE ON *.* TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${db}.* TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON AZURE TO $user"
# `CREATE TABLE ... AS azureBlobStorage(...)` needs the grant of the engine, as `ENGINE = AzureBlobStorage` does.
# It does not imply `WRITE ON AZURE`, so the structure inference below is still done with read access only.
${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON AzureBlobStorage TO $user"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.secret (marker String) ENGINE = Memory"

container_state()
{
    local out
    out=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM azureBlobStorage('${AZURE_CONN}', '$1', '*.csv', 'CSV', 'auto', 'marker String')" 2>&1)
    case "$out" in
        0) echo "container: exists" ;;
        *"container does not exist"*) echo "container: absent" ;;
        *) echo "container: unexpected: $(echo "$out" | head -1)" ;;
    esac
}

run_as_user()
{
    local name=$1 container=$2 query=$3
    echo "--- $name"
    container_state "$container"
    ${CLICKHOUSE_CLIENT} --user "$user" --query "$query" 2>&1 | grep -oE 'CANNOT_EXTRACT_TABLE_STRUCTURE|ACCESS_DENIED|^line\s+String$' | head -1
    container_state "$container"
}

c="${AZURE_CONT_PREFIX}directinsert"
run_as_user "INSERT INTO FUNCTION (no WRITE ON AZURE)" "$c" \
    "INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '$c', 'data.csv', 'CSV') VALUES ('m')"

c="${AZURE_CONT_PREFIX}describe"
run_as_user "DESCRIBE" "$c" \
    "DESCRIBE TABLE azureBlobStorage('${AZURE_CONN}', '$c', 'data.csv', 'CSV')"

c="${AZURE_CONT_PREFIX}describefixed"
run_as_user "DESCRIBE, format with a fixed schema" "$c" \
    "DESCRIBE TABLE azureBlobStorage('${AZURE_CONN}', '$c', 'data.txt', 'LineAsString') SETTINGS describe_compact_output = 1"

c="${AZURE_CONT_PREFIX}createas"
run_as_user "CREATE TABLE AS" "$c" \
    "CREATE TABLE ${db}.t_create_as AS azureBlobStorage('${AZURE_CONN}', '$c', 'data.csv', 'CSV')"

c="${AZURE_CONT_PREFIX}vipinsert"
run_as_user "INSERT INTO FUNCTION viewIfPermitted" "$c" \
    "INSERT INTO FUNCTION viewIfPermitted(SELECT marker FROM ${db}.secret ELSE azureBlobStorage('${AZURE_CONN}', '$c', 'data.csv', 'CSV')) VALUES ('m')"

c="${AZURE_CONT_PREFIX}nestedvipinsert"
run_as_user "INSERT INTO FUNCTION nested viewIfPermitted" "$c" \
    "INSERT INTO FUNCTION viewIfPermitted(SELECT marker FROM ${db}.secret ELSE viewIfPermitted(SELECT marker FROM ${db}.secret ELSE azureBlobStorage('${AZURE_CONN}', '$c', 'data.csv', 'CSV'))) VALUES ('m')"

echo "--- INSERT INTO FUNCTION by a user with WRITE ON AZURE still creates the container"
c="${AZURE_CONT_PREFIX}insertwithwrite"
container_state "$c"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '$c', 'data.txt', 'LineAsString') VALUES ('m')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM azureBlobStorage('${AZURE_CONN}', '$c', 'data.txt', 'LineAsString')"
container_state "$c"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
