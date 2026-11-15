#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Kafka client logs sent to the client must not contain memory addresses, neither in the configuration
# properties logged by ClickHouse (e.g. `log_cb`) nor in the librdkafka configuration dump (`debug = 'conf'`).
# No broker is needed: the logs are written when the consumer is created.

collection="${CLICKHOUSE_DATABASE}_kafka"

$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION $collection AS
        kafka_broker_list = '127.0.0.1:1',
        kafka_topic_list = 'topic',
        kafka_group_name = 'group',
        kafka_format = 'JSONEachRow',
        \`kafka.debug\` = 'conf';
    CREATE TABLE kafka_probe (x UInt64) ENGINE = Kafka($collection)
        SETTINGS kafka_poll_timeout_ms = 100, kafka_flush_interval_ms = 100;
"

logs=$($CLICKHOUSE_CLIENT --send_logs_level=trace -q "
    SELECT * FROM kafka_probe LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1
" 2>&1 >/dev/null | grep -E 'set property|\[rdk:' | sed "s/${CLICKHOUSE_DATABASE}/<database>/g")

echo "log_cb property hidden: $(grep -c 'set property log_cb:\[HIDDEN\]' <<< "$logs")"
echo "librdkafka configuration dump logged: $(grep -c 'log_cb = ' <<< "$logs" | sed 's/^[1-9][0-9]*$/1/')"
echo "lines with hexadecimal numbers: $(grep -cE '0x[0-9a-fA-F]' <<< "$logs")"

# The librdkafka mock cluster uses memory addresses as consumer group member ids, so its properties are rejected.
$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${collection}_mock AS
        kafka_broker_list = '127.0.0.1:1',
        kafka_topic_list = 'topic',
        kafka_group_name = 'group',
        kafka_format = 'JSONEachRow',
        \`kafka.test.mock.num.brokers\` = '1';
    CREATE TABLE kafka_mock (x UInt64) ENGINE = Kafka(${collection}_mock);
"
$CLICKHOUSE_CLIENT -q "SELECT * FROM kafka_mock SETTINGS stream_like_engine_allow_direct_select = 1" 2>&1 \
    | grep -o 'Kafka property test.mock.num.brokers is not allowed' | head -1

$CLICKHOUSE_CLIENT -q "
    DROP TABLE kafka_probe;
    DROP TABLE kafka_mock;
    DROP NAMED COLLECTION $collection;
    DROP NAMED COLLECTION ${collection}_mock;
"
