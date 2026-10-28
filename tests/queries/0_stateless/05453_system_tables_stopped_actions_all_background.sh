#!/usr/bin/env bash
# Tags: no-fasttest

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

stream_table="stopped_actions_all_background"
access_user="user_${CLICKHOUSE_DATABASE}_all_background"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS ${stream_table} SYNC;
        DROP USER IF EXISTS ${access_user};
    "
}
trap cleanup EXIT

# `Kafka` is not built in fast tests. The table is never read, only its local lock is inspected.
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE ${stream_table} (x UInt64) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'stopped_actions',
             kafka_group_name = 'stopped_actions', kafka_format = 'JSONEachRow';
    CREATE USER ${access_user};
    GRANT SYSTEM VIEWS ON ${CLICKHOUSE_DATABASE}.${stream_table} TO ${access_user};
    GRANT SYSTEM STREAMING ENGINES ON ${CLICKHOUSE_DATABASE}.${stream_table} TO ${access_user};
"

# The wildcard form fans out per table and skips the ones the caller may not control, so running it
# as a user granted on this table only keeps it from pausing the streaming tables of other tests.
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "SYSTEM STOP ALL BACKGROUND"
${CLICKHOUSE_CLIENT} --query "
    SELECT stopped_actions FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}' AND name = '${stream_table}';
"
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "SYSTEM START ALL BACKGROUND"
${CLICKHOUSE_CLIENT} --query "
    SELECT stopped_actions FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}' AND name = '${stream_table}';
"
