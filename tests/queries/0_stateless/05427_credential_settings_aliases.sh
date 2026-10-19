#!/usr/bin/env bash
# The server credential settings apply to Azure as well as S3, so their old `s3_` names are kept as aliases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    SELECT name, alias_for FROM system.settings
    WHERE name IN ('allow_server_credentials_in_user_queries', 's3_allow_server_credentials_in_user_queries')
    ORDER BY name"

# Setting the old name changes the setting it points to.
$CLICKHOUSE_CLIENT -q "
    SET s3_allow_server_credentials_in_user_queries = 1;
    SELECT getSetting('allow_server_credentials_in_user_queries')"

# A server setting configured under its old name keeps working.
CONFIG="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"
cat > "$CONFIG" <<EOF
<clickhouse>
    <s3_load_table_anonymously_if_credentials_restricted>0</s3_load_table_anonymously_if_credentials_restricted>
    <s3_allow_server_credentials_for_system_table_disks>1</s3_allow_server_credentials_for_system_table_disks>
</clickhouse>
EOF
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "
    SELECT name, value FROM system.server_settings
    WHERE name IN ('load_table_anonymously_if_credentials_restricted', 'allow_server_credentials_for_system_table_disks')
    ORDER BY name"

# But it may not be configured under both names.
cat > "$CONFIG" <<EOF
<clickhouse>
    <allow_server_credentials_for_system_table_disks>1</allow_server_credentials_for_system_table_disks>
    <s3_allow_server_credentials_for_system_table_disks>1</s3_allow_server_credentials_for_system_table_disks>
</clickhouse>
EOF
$CLICKHOUSE_LOCAL --config-file "$CONFIG" -q "SELECT 1" 2>&1 | grep -o "configured under more than one name" | head -1

rm -f "$CONFIG"
