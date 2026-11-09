#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection_name="${CLICKHOUSE_DATABASE}_map_value"

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION $collection_name AS headers = {'k1':'v1'}, a = 1"
$CLICKHOUSE_CLIENT -q "SELECT mapKeys(collection) FROM system.named_collections WHERE name = '$collection_name'"

$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION $collection_name SET headers = {'k2':'v2'}, b = 2"
$CLICKHOUSE_CLIENT -q "SELECT mapKeys(collection) FROM system.named_collections WHERE name = '$collection_name'"

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION $collection_name"
