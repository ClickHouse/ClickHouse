#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Properties in the `consumer` section of a Kafka named collection and in the `kafka_topic` section of a consumed topic
# are set for the consumer, properties in the `producer` section are not. No broker is needed: the properties are set
# when the consumer is created. The topic is `topic`, so its legacy section `kafka_topic` has the name of a new syntax section.

collection="${CLICKHOUSE_DATABASE}_kafka"

$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION $collection AS
        kafka_broker_list = '127.0.0.1:1',
        kafka_topic_list = 'topic',
        kafka_group_name = 'group',
        kafka_format = 'JSONEachRow',
        \`kafka.consumer.fetch_min_bytes\` = '777',
        \`kafka.producer.linger_ms\` = '777',
        \`kafka.kafka_topic.name\` = 'topic',
        \`kafka.kafka_topic.retry_backoff_ms\` = '777',
        \`kafka.kafka_topic[1].name\` = 'baseball',
        \`kafka.kafka_topic[1].reconnect_backoff_ms\` = '777';
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

# Nested keys given in the table definition replace or add to the ones of the collection, also after the collection is changed.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE kafka_probe;
    CREATE TABLE kafka_probe (x UInt64) ENGINE = Kafka($collection,
        \`kafka.consumer.fetch_min_bytes\` = '888',
        \`kafka.consumer.fetch_wait_max_ms\` = '888',
        \`kafka.kafka_topic.retry_backoff_ms\` = '888')
        SETTINGS kafka_poll_timeout_ms = 100, kafka_flush_interval_ms = 100;
    ALTER NAMED COLLECTION $collection SET
        \`kafka.consumer.fetch_min_bytes\` = '999',
        \`kafka.consumer.fetch_error_backoff_ms\` = '999';
"

logs=$($CLICKHOUSE_CLIENT --send_logs_level=trace -q "
    SELECT * FROM kafka_probe LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1
" 2>&1 >/dev/null)

echo "overridden consumer property set: $(grep -c 'Consumer set property fetch.min.bytes:888' <<< "$logs")"
echo "added consumer property set: $(grep -c 'Consumer set property fetch.wait.max.ms:888' <<< "$logs")"
echo "overridden topic property set: $(grep -c 'Consumer set property retry.backoff.ms:888' <<< "$logs")"
echo "stored values set: $(grep -cE 'Consumer set property (fetch.min.bytes|retry.backoff.ms):777' <<< "$logs")"
echo "changed collection property set: $(grep -c 'Consumer set property fetch.error.backoff.ms:999' <<< "$logs")"
echo "changed overridden property set: $(grep -c 'Consumer set property fetch.min.bytes:999' <<< "$logs")"

# A secret nested key given in the table definition is hidden in it, also when spelled with an index or redundant dots.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE kafka_secret (x UInt64) ENGINE = Kafka($collection,
        \`kafka.consumer.sasl_password\` = 'secret_05621',
        \`kafka[0].producer.sasl_password\` = 'secret_05621',
        \`kafka[0]consumer.sasl_password\` = 'secret_05621',
        \`.kafka..kafka_topic.sasl_password\` = 'secret_05621')
"
echo "secret shown: $($CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE kafka_secret" | grep -c 'secret_05621')"

# The overrides of a collection from the server configuration file are checked as the keys of a collection created by SQL.
echo "file path override rejected: $($CLICKHOUSE_CLIENT -q "
    CREATE TABLE kafka_paths (x UInt64) ENGINE = Kafka(kafka_config_file_paths, \`kafka.ssl_key_location\` = '/etc/passwd')
" 2>&1 | grep -c 'Kafka property \`ssl.key.location\` can only be specified')"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE kafka_config (x UInt64) ENGINE = Kafka(kafka_config_file_paths, \`kafka.consumer.fetch_min_bytes\` = '555')
        SETTINGS kafka_poll_timeout_ms = 100, kafka_flush_interval_ms = 100
"
logs=$($CLICKHOUSE_CLIENT --send_logs_level=trace -q "
    SELECT * FROM kafka_config LIMIT 1 SETTINGS stream_like_engine_allow_direct_select = 1
" 2>&1 >/dev/null)
echo "override of config collection set: $(grep -c 'Consumer set property fetch.min.bytes:555' <<< "$logs")"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE kafka_secret;
    DROP TABLE kafka_config;
    DROP TABLE IF EXISTS kafka_paths;
    DROP TABLE kafka_probe;
    DROP NAMED COLLECTION $collection;
"
