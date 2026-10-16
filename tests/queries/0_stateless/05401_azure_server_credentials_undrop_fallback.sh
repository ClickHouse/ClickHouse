#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database
# Tag no-fasttest: needs USE_AZURE_BLOB_STORAGE and Azurite.
# Tag no-ordinary-database, no-replicated-database: needs `UNDROP TABLE`.
#
# A table reloaded from its stored definition, here by `UNDROP`, whose credentials now resolve to the server's own
# identity is loaded with a client that has no credentials: it loads, and its queries fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
NC="azure_undrop_${DB}"
TABLE="azure_undrop_${DB}"

AZURITE_URL="http://localhost:10000/devstoreaccount1"
AZURITE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=${AZURITE_KEY};BlobEndpoint=${AZURITE_URL};"
CONTAINER="cont-$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | tr _ -)"

cleanup() {
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS ${TABLE} SYNC;
        DROP NAMED COLLECTION IF EXISTS ${NC};
    " 2>/dev/null
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -m -q "
    INSERT INTO FUNCTION azureBlobStorage('${CONN_STRING}', '${CONTAINER}', 'f.csv', 'CSV', 'auto', 'x UInt8')
        SELECT 7 SETTINGS azure_truncate_on_insert = 1;
    CREATE NAMED COLLECTION ${NC} AS
        storage_account_url = '${AZURITE_URL}', container = '${CONTAINER}', blob_path = 'f.csv',
        account_name = 'devstoreaccount1', account_key = '${AZURITE_KEY}';
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = AzureBlobStorage(${NC}, format = 'CSV');
    SELECT * FROM ${TABLE};
"

# Without the account key the stored definition resolves to the server's own identity.
$CLICKHOUSE_CLIENT -m -q "
    SET database_atomic_wait_for_drop_and_detach_synchronously = 0;
    DROP TABLE ${TABLE};
    ALTER NAMED COLLECTION ${NC} DELETE account_name, account_key;
"

if $CLICKHOUSE_CLIENT --send_logs_level=warning -q "UNDROP TABLE ${TABLE}" 2>&1 \
    | grep -qF 'Loading this table with an Azure client that has no credentials'; then
    echo "undrop: loaded without credentials"
else
    echo "undrop: not loaded without credentials"
fi

$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${TABLE}"

# The request reaches Azurite unauthenticated and is denied there, not refused by ClickHouse.
output=$($CLICKHOUSE_CLIENT --azure_sdk_max_retries=0 -q "SELECT * FROM ${TABLE}" 2>&1)
if [[ $? -eq 0 ]]; then
    echo "select: succeeded"
elif grep -qF 'ACCESS_DENIED' <<< "${output}"; then
    echo "select: refused by ClickHouse"
else
    echo "select: denied by the endpoint"
fi
