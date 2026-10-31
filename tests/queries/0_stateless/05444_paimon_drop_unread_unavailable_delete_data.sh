#!/usr/bin/env bash
# Tags: no-fasttest
# `data_lake_delete_data_on_drop` must not load the metadata of a data lake that does not delete its data on drop,
# so an unread `PaimonLocal` table whose source is unavailable can still be dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_unavailable_paimon"

${CLICKHOUSE_CLIENT} --send_logs_level=error --allow_experimental_paimon_storage_engine=1 \
    --query "ATTACH TABLE t UUID '$(cat /proc/sys/kernel/random/uuid)' (x Int64) ENGINE = PaimonLocal('${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t SYNC SETTINGS data_lake_delete_data_on_drop = 1"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't'"
