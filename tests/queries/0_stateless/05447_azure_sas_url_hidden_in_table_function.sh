#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no `azureBlobStorage` table function

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A storage account url carrying a credential of its own (a SAS `?sig=...`, `user:key@`), or a value that is
# not a plain string literal, is hidden whole in the Azure table functions, positional or as a named-collection
# override, as the `AzureBlobStorage` engine and the backup destination already do.
while read -r query; do
    $CLICKHOUSE_FORMAT --oneline --query "$query"
done <<'EOF'
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/?sp=r&sig=plain_sas_signature', 'container', 'blob')
SELECT * FROM azureBlobStorage('https://user:plain_password@account.blob.core.windows.net', 'container', 'blob')
SELECT * FROM azureBlobStorage(concat('https://account.blob.core.windows.net/?sig=', 'plain_sas_signature'), 'container', 'blob')
SELECT * FROM azureBlobStorageCluster('cluster', 'https://account.blob.core.windows.net/?sp=r&sig=plain_sas_signature', 'container', 'blob')
SELECT * FROM icebergAzure('https://account.blob.core.windows.net/?sp=r&sig=plain_sas_signature', 'container', 'path')
SELECT * FROM azureBlobStorage(azure_conf, storage_account_url = 'https://account.blob.core.windows.net/?sp=r&sig=plain_sas_signature', container = 'container', blob_path = 'blob')
SELECT * FROM azureBlobStorageCluster('cluster', azure_conf, connection_string = 'https://account.blob.core.windows.net/?sp=r&sig=plain_sas_signature', container = 'container')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', 'CSV')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', 'account', 'plain_account_key')
SELECT * FROM azureBlobStorage(azure_conf, storage_account_url = 'https://account.blob.core.windows.net', container = 'container')
EOF

# `SELECT` on a view implies `SHOW COLUMNS`, so a user granted only that sees the view definition through
# `SHOW CREATE TABLE` and `system.tables`, but not the SAS in it.
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "CREATE VIEW ${CLICKHOUSE_DATABASE}.v_sas AS SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/?sp=r&sig=plain_sas_signature', 'container', 'file.csv', 'CSV', 'none', 'id UInt8')"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'hello'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_sas TO ${user}"

$CLICKHOUSE_CLIENT --user "${user}" --password hello -q "SHOW CREATE TABLE ${CLICKHOUSE_DATABASE}.v_sas" | grep -o -e 'plain_sas_signature' -e 'HIDDEN'
$CLICKHOUSE_CLIENT --user "${user}" --password hello -q "SELECT countIf(create_table_query LIKE '%plain_sas_signature%'), countIf(create_table_query LIKE '%[HIDDEN]%') FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'v_sas'"

# `EXPLAIN AST optimize = 1` expands the view, which executes the table function and needs more than `SELECT`
# on the view, so it is run as the default user, who sees no secrets either without
# `format_display_secrets_in_show_and_select`.
$CLICKHOUSE_CLIENT --format_display_secrets_in_show_and_select 0 -q "SELECT countIf(explain LIKE '%plain_sas_signature%'), countIf(explain LIKE '%[HIDDEN]%') FROM (EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.v_sas)"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
