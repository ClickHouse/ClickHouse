#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs USE_AZURE_BLOB_STORAGE and the `azure` disks from tests/config/config.d/azure_storage_conf.xml.
#
# The error for a `table_disk` on an unsupported object storage disk names the disk instead of dumping
# its whole endpoint configuration.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ENDPOINT_CONFIG="DefaultEndpointsProtocol="

for disk in azure cached_azure cached_azure_encrypted; do
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${disk}"
    output=$($CLICKHOUSE_CLIENT --query_id "${query_id}" -q "
        CREATE TABLE t_${disk} (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS table_disk = 1, disk = '${disk}'" 2>&1)
    grep -q -F 'BAD_ARGUMENTS' <<< "${output}" && error=1 || error=0
    grep -q -F "${ENDPOINT_CONFIG}" <<< "${output}" && verbose=1 || verbose=0
    echo "${disk}: error ${error}, endpoint config in message ${verbose}"
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
    SELECT replaceOne(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', '') AS disk, countIf(position(exception, '${ENDPOINT_CONFIG}') > 0)
    FROM system.query_log
    WHERE event_date >= yesterday() AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_') AND type != 'QueryStart'
    GROUP BY disk
    ORDER BY disk
"
