#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Properties in the `consumer` section of a Kafka named collection and in the `kafka_topic` section of a consumed topic
# are set for the consumer, properties in the `producer` section are not. No broker is needed: the properties are set
# when the consumer is created.

collection="${CLICKHOUSE_DATABASE}_kafka"

$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION $collection AS
        kafka_broker_list = '127.0.0.1:1',
        kafka_topic_list = 'football',
        kafka_group_name = 'group',
        kafka_format = 'JSONEachRow',
        \`kafka.consumer.fetch_min_bytes\` = '777',
        \`kafka.producer.linger_ms\` = '777',
        \`kafka.kafka_topic.name\` = 'football',
        \`kafka.kafka_topic.retry_backoff_ms\` = '777',
        \`kafka.kafka_topic_baseball.name\` = 'baseball',
        \`kafka.kafka_topic_baseball.reconnect_backoff_ms\` = '777';
    CREATE TABLE kafka_probe (x UInt64) ENGINE = Kafka($collection)
        SETTINGS kafka_poll_timeout_ms = 100, kafka_flush_interval_ms = 100;
"

logs=$($CLICKHOUSE_CLIENT --send_logs_level=trace -q "
    SELECT * FROM kafka_probe LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1
" 2>&1 >/dev/null)

echo "consumer property set: $(grep -c 'Consumer set property fetch.min.bytes:777' <<< "$logs")"
echo "producer property set for consumer: $(grep -c 'Consumer set property linger.ms:777' <<< "$logs")"
echo "topic property set: $(grep -c 'Consumer set property retry.backoff.ms:777' <<< "$logs")"
echo "other topic property set: $(grep -c 'Consumer set property reconnect.backoff.ms:777' <<< "$logs")"
echo "rejected properties: $(grep -c 'No such configuration property' <<< "$logs")"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE kafka_probe;
    DROP NAMED COLLECTION $collection;
"
