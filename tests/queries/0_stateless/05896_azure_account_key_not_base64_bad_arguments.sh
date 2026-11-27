#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: needs Azurite
# - no-replicated-database: uses ON CLUSTER

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

AZURE_URL="http://localhost:10000/devstoreaccount1"
AZURE_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
cont="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"
nc="nc_${CLICKHOUSE_DATABASE}"
db=${CLICKHOUSE_DATABASE}

# Prints the label, then OK if the statement succeeds, else the code it fails with (BAD_ARGUMENTS or STD_EXCEPTION).
# Further arguments are passed to the client.
run()
{
    local label=$1 query=$2 out
    if out=$(${CLICKHOUSE_CLIENT} "${@:3}" --query "$query" 2>&1); then
        echo "$label OK"
    else
        echo "$label $(grep -o -E 'BAD_ARGUMENTS|STD_EXCEPTION' <<< "$out" | head -n1)"
    fi
}

${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS $nc"

run args "CREATE TABLE ${db}.t_args (a String) ENGINE = AzureBlobStorage('$AZURE_URL', '$cont', 'x.csv', 'devstoreaccount1', 'NotBase64Key1', 'CSV')"
run bad_char "CREATE TABLE ${db}.t_bad_char (a String) ENGINE = AzureBlobStorage('$AZURE_URL', '$cont', 'x.csv', 'devstoreaccount1', 'ab!d', 'CSV')"
run connection_string "CREATE TABLE ${db}.t_connection_string (a String) ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=NotBase64Key1;BlobEndpoint=$AZURE_URL;', '$cont', 'x.csv', 'CSV')"
run no_account_name "CREATE TABLE ${db}.t_no_account_name (a String) ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountKey=$AZURE_KEY;BlobEndpoint=$AZURE_URL;', '$cont', 'x.csv', 'CSV')"
run create_as "CREATE TABLE ${db}.t_as AS azureBlobStorage('$AZURE_URL', '$cont', 'x.csv', 'devstoreaccount1', 'NotBase64Key1', 'CSV', 'auto', 'a String')"
run insert_function "INSERT INTO FUNCTION azureBlobStorage('$AZURE_URL', '$cont', 'x.csv', 'devstoreaccount1', 'NotBase64Key1', 'CSV', 'auto', 'a String') SELECT 'x'"
run disk "CREATE TABLE ${db}.t_disk (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS disk = disk(type = 'azure_blob_storage', storage_account_url = '$AZURE_URL', container_name = '$cont', account_name = 'devstoreaccount1', account_key = 'NotBase64Key1')"
run disk_connection_string_and_key "CREATE TABLE ${db}.t_disk2 (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS disk = disk(type = 'azure_blob_storage', connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=$AZURE_KEY;BlobEndpoint=$AZURE_URL;', container_name = '$cont', account_name = 'devstoreaccount1', account_key = 'NotBase64Key1')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.t_src (a Int8) ENGINE = MergeTree ORDER BY a"
run disk_container_exists "CREATE TABLE ${db}.t_disk3 (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS disk = disk(type = 'azure_blob_storage', storage_account_url = '$AZURE_URL', container_name = '$cont', container_already_exists = 1, account_name = 'devstoreaccount1', account_key = 'NotBase64Key1')"
run backup_no_create "BACKUP TABLE ${db}.t_src TO AzureBlobStorage('$AZURE_URL', '$cont', 'backup_${db}', 'devstoreaccount1', 'NotBase64Key1') SETTINGS azure_attempt_to_create_container = 0"
run on_cluster "CREATE TABLE ${db}.t_on_cluster ON CLUSTER test_shard_localhost (a String) ENGINE = AzureBlobStorage('$AZURE_URL', '$cont', 'x.csv', 'devstoreaccount1', 'NotBase64Key1', 'CSV') FORMAT Null" --distributed_ddl_output_mode=throw

# A table whose stored named collection gets an invalid key still loads; only a new one is rejected.
# ast_fuzzer_any_query = 0: a fuzzed DETACH or clone of a table that uses the named collection would keep it from being dropped.
${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION $nc AS storage_account_url = '$AZURE_URL', container = '$cont', blob_path = 'x.csv', account_name = 'devstoreaccount1', account_key = '$AZURE_KEY', format = 'CSV'"
run create_valid_key "CREATE TABLE ${db}.t_nc (a String) ENGINE = AzureBlobStorage($nc)" --ast_fuzzer_any_query=0
${CLICKHOUSE_CLIENT} --query "ALTER NAMED COLLECTION $nc SET account_key = 'NotBase64Key1'"
${CLICKHOUSE_CLIENT} --ast_fuzzer_any_query=0 --query "DETACH TABLE ${db}.t_nc"
run attach "ATTACH TABLE ${db}.t_nc" --ast_fuzzer_any_query=0
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db}' AND name = 't_nc'"
run create_invalid_key "CREATE TABLE ${db}.t_nc2 (a String) ENGINE = AzureBlobStorage($nc)" --ast_fuzzer_any_query=0

${CLICKHOUSE_CLIENT} --ast_fuzzer_any_query=0 --query "DROP TABLE IF EXISTS ${db}.t_nc, ${db}.t_nc2, ${db}.t_src"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS $nc"
