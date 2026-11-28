#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An error in the background streaming of a Kafka table with offsets in Keeper is shown in system.kafka_consumers and logged
# under the name of the table. No broker is needed: the property added to the collection is rejected before the connection.

collection="${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mv; DROP TABLE IF EXISTS dst; DROP TABLE IF EXISTS kafka; DROP NAMED COLLECTION IF EXISTS ${collection};"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${collection} AS
    kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'topic', kafka_group_name = '${collection}', kafka_format = 'JSONEachRow'"
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper=1 -q "CREATE TABLE kafka (x UInt64) ENGINE = Kafka(${collection})
    SETTINGS kafka_keeper_path = '/clickhouse/test/${CLICKHOUSE_TEST_UNIQUE_NAME}', kafka_replica_name = 'r1'"
$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION ${collection} SET \`kafka.ssl_key_location\` = '/etc/passwd'"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM kafka;"

for _ in {1..60}
do
    exceptions=$($CLICKHOUSE_CLIENT -q "SELECT sum(length(exceptions.text)) FROM system.kafka_consumers
        WHERE database = currentDatabase() AND table = 'kafka'")
    [[ "${exceptions:-0}" -gt 0 ]] && break
    sleep 1
done

$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(arrayExists(t -> t LIKE '%ssl.key.location%', exceptions.text))
    FROM system.kafka_consumers WHERE database = currentDatabase() AND table = 'kafka'"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND level = 'Error'
        AND logger_name LIKE concat('StorageKafka2 (', currentDatabase(), '.kafka%') AND message LIKE '%ssl.key.location%'"
