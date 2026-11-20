#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Azure table functions and engines, whose arguments it then hides whole

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-format` hides the credentials in the arguments of the Azure table functions and engines and in the
# `SETTINGS` clause of `AzureQueue`, wherever the parser reads them from.
while read -r query; do
    $CLICKHOUSE_FORMAT --oneline --query "$query"
done <<'EOF'
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', extra_credentials(client_id = 'id', tenant_id = 'tid'), 'account', 'plain_account_key')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature')
SELECT * FROM azureBlobStorageCluster('cluster', 'https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature', SETTINGS input_format_csv_skip_first_lines = 0)
SELECT * FROM azureBlobStorageCluster('cluster', 'https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature', SETTINGS input_format_csv_skip_first_lines = 0)
SELECT * FROM icebergAzure('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature', SETTINGS input_format_csv_skip_first_lines = 0)
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature', partition_strategy = 'none')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature' = 'x')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature', extra_credentials(client_id = 'id', client_secret = 'plain_client_secret', role_arn = 'plain_role_arn'))
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', account_key = 'plain_account_key')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', partition_strategy = 'none', 'plain_account_key')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', extra_credentials(client_id = 'id'), extra_credentials(tenant_id = 'tid'), 'plain_account_key', 'CSV')
SELECT * FROM azureBlobStorageCluster(extra_credentials(client_id = 'id'), 'https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', extra_credentials('plain_client_secret' = 'x', client_id = 'plain_client_id'))
SELECT * FROM azureBlobStorage('AccountName=account;AccountKey=plain_account_key;BlobEndpoint=https://account.blob.core.windows.net', 'container', 'blob', 'account', 'plain_account_key')
SELECT * FROM azureBlobStorage('AccountName=account;AccountKey=plain_account_key;SharedAccessSignature=sp=r&sig=plain_sas_signature', 'container', 'blob')
SELECT * FROM azureBlobStorage('AccountName=account;AccountKey=plain_account_key;AccountKey=plain_account_key_2', 'container', 'blob')
SELECT * FROM azureBlobStorage('AccountName=account;BlobEndpoint=https://account.blob.core.windows.net/?sig=plain_sas_signature', 'container', 'blob')
SELECT * FROM azureBlobStorage('account.blob.core.windows.net/?sig=plain_sas_signature', 'container', 'blob')
SELECT * FROM azureBlobStorage('HTTPS://user:plain_password=x@account.blob.core.windows.net/', 'container', 'blob')
SELECT * FROM azureBlobStorage(azure_conf, connection_string = 'AccountName=account;AccountKey=plain_account_key', account_key = 'plain_account_key')
SELECT * FROM azureBlobStorage(azure_conf, client_secret = 'plain_client_secret', blob_path = 'blob')
SELECT * FROM azureBlobStorage(azure_conf, container = headers('Authorization' = 'plain_header_secret'), blob_path = 'blob')
SELECT * FROM azureBlobStorage(azure_conf, connection_string = 'AccountName=account;AccountKey=plain_account_key', storage_account_url = 'AccountName=account;AccountKey=plain_account_key_2')
SELECT * FROM azureBlobStorageCluster('cluster', azure_conf, connection_string = concat('AccountName=account;AccountKey=', 'plain_account_key'), connection_string = 'AccountName=account;AccountKey=plain_account_key_2')
CREATE TABLE test_azure (key UInt64) ENGINE = AzureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature', partition_strategy = 'none')
CREATE TABLE test_azure (key UInt64) ENGINE = AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', partition_strategy = 'none', 'plain_account_key')
CREATE TABLE test_azure (key UInt64) ENGINE = AzureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', 'CSV', extra_credentials(client_id = 'id', client_secret = 'plain_client_secret', role_arn = 'plain_role_arn'))
CREATE TABLE test_azure (key UInt64) ENGINE = AzureBlobStorage(azure_conf, sas_token = 'plain_sas_signature')
CREATE TABLE test_azure (key UInt64) ENGINE = AzureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature' = 'x')
CREATE TABLE test_azure_queue (key UInt64) ENGINE = AzureQueue('https://account.blob.core.windows.net', 'container', '*', 'account', 'plain_account_key' = 'x')
CREATE TABLE test_azure_queue (key UInt64) ENGINE = AzureQueue('https://account.blob.core.windows.net', 'container', '*', 'CSV') SETTINGS after_processing_move_connection_string = 'HTTPS://account.blob.core.windows.net/?sig=plain_sas_signature'
CREATE TABLE test_azure_queue (key UInt64) ENGINE = AzureQueue('https://account.blob.core.windows.net', 'container', '*', 'CSV') SETTINGS after_processing_move_connection_string = 'https://user:plain_password=x@account.blob.core.windows.net/'
CREATE TABLE test_azure_queue (key UInt64) ENGINE = AzureQueue('https://account.blob.core.windows.net', 'container', '*', 'CSV') SETTINGS after_processing_move_connection_string = 'AccountName=account;AccountKey=plain_account_key;AccountKey=plain_account_key_2'
EOF

# The query tree hides an argument masked whole, not only the right side of a comparison.
$CLICKHOUSE_CLIENT -q "SELECT countIf(explain LIKE '%plain_sas_signature%'), countIf(explain LIKE '%[HIDDEN]%') FROM (EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_signature' = 'x'))"
$CLICKHOUSE_CLIENT -q "SELECT countIf(explain LIKE '%plain_sas_signature%'), countIf(explain LIKE '%[HIDDEN]%') FROM (EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(azure_conf, 'sp=r&sig=plain_sas_signature' = 'x'))"

# Nothing inside an Azure `extra_credentials(...)` is dumped, keys included.
for explain in "AST" "QUERY TREE run_passes = 0"; do
    $CLICKHOUSE_CLIENT -q "SELECT countIf(explain LIKE '%plain_client_%'), countIf(explain LIKE '%[HIDDEN]%') FROM (EXPLAIN $explain SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', extra_credentials('plain_client_secret' = 'x', client_id = 'plain_client_id')))"
done

# The statement is logged with the credential hidden.
$CLICKHOUSE_CLIENT -q "EXPLAIN AST SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/container/blob.csv', 'sp=r&sig=plain_sas_logged')" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "SELECT countIf(query LIKE '%plain_sas_logged%'), countIf(query LIKE '%[HIDDEN]%') FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'EXPLAIN AST%azureBlobStorage%' AND type = 'QueryFinish' AND is_initial_query"
