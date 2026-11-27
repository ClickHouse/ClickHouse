#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Requires `Kafka` and a local SQL-created named collection.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="${CLICKHOUSE_TEST_UNIQUE_NAME}"
query_id="${collection}_create"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kafka_xml_override; DROP NAMED COLLECTION IF EXISTS ${collection}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${collection} AS kafka_broker_list = '127.0.0.1:1',
    kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow'"

$CLICKHOUSE_CLIENT --query_id="$query_id" --log_queries=1 --log_queries_probability=1 -q "
    CREATE TABLE kafka_xml_override (x UInt8) ENGINE = Kafka(${collection},
        \`kafka[0].sasl_oauthbearer_client_secret\` = 'xml_mask_canary',
        \`kafka.client_id\` = 'visible_client')"

check_masked()
{
    [[ $1 == *"[HIDDEN]"* && $1 != *xml_mask_canary* && $1 == *visible_client* ]]
}

ddl=$($CLICKHOUSE_CLIENT --format_display_secrets_in_show_and_select=0 -q 'SHOW CREATE TABLE kafka_xml_override')
check_masked "$ddl"
echo 'SHOW CREATE masks the override'

$CLICKHOUSE_CLIENT -q 'SYSTEM FLUSH LOGS query_log'
logged=$($CLICKHOUSE_CLIENT -q "SELECT query FROM system.query_log WHERE event_date >= yesterday()
    AND type = 'QueryFinish' AND query_id = '${query_id}' AND current_database = currentDatabase()")
check_masked "$logged"
echo 'query_log masks the override'
