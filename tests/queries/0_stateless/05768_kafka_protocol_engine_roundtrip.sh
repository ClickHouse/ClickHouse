#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Kafka` table engine is not in the fast test build.

# A MergeTree table is a topic of the Kafka-compatible interface of the server (`kafka_port`).
# The `Kafka` table engine (librdkafka) consumes from it and produces into it, as from a Kafka broker.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BROKER="127.0.0.1:${CLICKHOUSE_PORT_KAFKA}"
TOPIC="${CLICKHOUSE_DATABASE}.topic"

function wait_for()
{
    local query=$1
    local expected=$2
    for _ in {1..1200}; do
        if [ "$($CLICKHOUSE_CLIENT -q "$query")" == "$expected" ]; then
            return
        fi
        sleep 0.1
    done
    echo "Timeout waiting for: $query"
}

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE topic
    (
        partition UInt32,
        key Nullable(String),
        value Nullable(String),
        timestamp DateTime64(3),
        headers Map(String, String),
        CONSTRAINT partition_count CHECK partition < 3
    )
    ENGINE = MergeTree PARTITION BY partition ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    -- The records inserted by SQL are visible to Kafka consumers.
    INSERT INTO topic (partition, key, value, timestamp, headers)
        SELECT number % 3, 'k' || toString(number), 'sql ' || toString(number), '2026-01-01 00:00:00', map('n', toString(number))
        FROM numbers(30);

    CREATE TABLE result (value String, key String, partition UInt64, offset UInt64, timestamp Nullable(DateTime64(3)), header_names Array(String), header_values Array(String))
    ENGINE = MergeTree ORDER BY tuple();

    CREATE TABLE consumer (value String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '${BROKER}', kafka_topic_list = '${TOPIC}', kafka_group_name = '${CLICKHOUSE_DATABASE}',
        kafka_format = 'RawBLOB', kafka_flush_interval_ms = 100;

    CREATE MATERIALIZED VIEW mv TO result AS
        SELECT value, _key AS key, _partition AS partition, _offset AS offset, _timestamp_ms AS timestamp,
            _headers.name AS header_names, _headers.value AS header_values
        FROM consumer;
"

wait_for "SELECT count() FROM result" 30

echo "--- consumed the records inserted by SQL"
$CLICKHOUSE_CLIENT -q "SELECT partition, count(), arraySort(groupArray(key))[1], min(timestamp) FROM result GROUP BY partition ORDER BY partition"
$CLICKHOUSE_CLIENT -q "SELECT key, value, header_names, header_values FROM result WHERE key = 'k7'"

echo "--- the Kafka offset is _block_number * 2^32 + _block_offset"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM result AS r INNER JOIN topic AS t ON r.value = t.value
    WHERE r.offset = t._block_number * 4294967296 + t._block_offset AND r.partition = t.partition"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE producer (n UInt64, s String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '${BROKER}', kafka_topic_list = '${TOPIC}', kafka_group_name = '${CLICKHOUSE_DATABASE}_producer',
        kafka_format = 'JSONEachRow';

    INSERT INTO producer SELECT number, 'produced ' || toString(number) FROM numbers(20);
"

echo "--- the produced records are inserted into the table"
wait_for "SELECT count() FROM topic" 50
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(JSONExtractUInt(value, 'n')), countIf(timestamp > '2026-01-01 00:00:00') FROM topic WHERE value LIKE '{%'"

echo "--- and consumed"
wait_for "SELECT count() FROM result" 50
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(JSONExtractUInt(value, 'n')) FROM result WHERE value LIKE '{%'"
$CLICKHOUSE_CLIENT -q "SELECT count() = uniqExact(partition, offset) FROM result"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv;
    DROP TABLE consumer;
    DROP TABLE producer;
    DROP TABLE result;
    DROP TABLE topic;
"
