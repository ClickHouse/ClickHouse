#!/usr/bin/env bash
# Tags: no-shared-merge-tree, no-object-storage, no-s3-storage
# no-shared-merge-tree, no-object-storage, no-s3-storage: UNIQUE KEY needs a plain MergeTree on a disk with transactions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A stored UNIQUE KEY + TTL table (which a new definition cannot have) still loads. Its stored definition
# is rewritten from a UNIQUE KEY table without TTL and attached back with the short ATTACH.
# MATERIALIZE TTL on it is rejected, REMOVE TTL is allowed.

$CLICKHOUSE_CLIENT --enable_unique_key 1 -q "CREATE TABLE uk_ttl_attach (id UInt64, d Date, v String) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)"
metadata_file=$($CLICKHOUSE_CLIENT -q "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'uk_ttl_attach'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE uk_ttl_attach"
sed -i.bak 's/^UNIQUE KEY (id)$/UNIQUE KEY (id) TTL d + toIntervalDay(1)/' "$metadata_file" && rm -f "$metadata_file.bak"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE uk_ttl_attach"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'uk_ttl_attach' AND create_table_query LIKE '%TTL%'"
$CLICKHOUSE_CLIENT -q "ALTER TABLE uk_ttl_attach MATERIALIZE TTL; -- { serverError SUPPORT_IS_DISABLED }"
$CLICKHOUSE_CLIENT -q "ALTER TABLE uk_ttl_attach REMOVE TTL"
$CLICKHOUSE_CLIENT -q "DROP TABLE uk_ttl_attach SYNC"
