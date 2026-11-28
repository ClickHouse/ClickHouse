#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Kafka` table engine is not in the fast test build.

# A MergeTree table of arbitrary structure is a topic of the Kafka-compatible interface (`kafka_port`):
# every row is a message in a format, the default `JSONEachRow` or the one of the table setting `kafka_format`.
# The columns `key` and `timestamp` are the key and the timestamp of the record, and are not in the message.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BROKER="127.0.0.1:${CLICKHOUSE_PORT_KAFKA}"

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
    CREATE TABLE events (id UInt64, name String, amount Decimal(10, 2), tags Array(String), timestamp DateTime64(3))
    ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO events SELECT number, 'event ' || toString(number), number / 4, arrayMap(x -> toString(x), range(number % 3)), '2026-01-01 00:00:00'
        FROM numbers(10);

    CREATE TABLE csv_events (p UInt8, key String, a UInt32, b String, CONSTRAINT partition_count CHECK p < 2)
    ENGINE = MergeTree PARTITION BY p ORDER BY a
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, kafka_format = 'CSV';

    INSERT INTO csv_events SELECT number % 2, 'k' || toString(number), number, 'b,' || toString(number) FROM numbers(6);

    CREATE TABLE result (id UInt64, name String, amount Decimal(10, 2), tags Array(String), timestamp Nullable(DateTime64(3)))
    ENGINE = MergeTree ORDER BY id;

    CREATE TABLE consumer (id UInt64, name String, amount Decimal(10, 2), tags Array(String))
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '${BROKER}', kafka_topic_list = '${CLICKHOUSE_DATABASE}.events', kafka_group_name = '${CLICKHOUSE_DATABASE}_events',
        kafka_format = 'JSONEachRow', kafka_flush_interval_ms = 100;

    CREATE MATERIALIZED VIEW mv TO result AS SELECT *, _timestamp_ms AS timestamp FROM consumer;

    CREATE TABLE csv_result (a UInt32, b String, key String, partition UInt64) ENGINE = MergeTree ORDER BY a;

    CREATE TABLE csv_consumer (a UInt32, b String)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '${BROKER}', kafka_topic_list = '${CLICKHOUSE_DATABASE}.csv_events', kafka_group_name = '${CLICKHOUSE_DATABASE}_csv',
        kafka_format = 'CSV', kafka_flush_interval_ms = 100;

    CREATE MATERIALIZED VIEW csv_mv TO csv_result AS SELECT a, b, _key AS key, _partition AS partition FROM csv_consumer;
"

wait_for "SELECT count() FROM result" 10
wait_for "SELECT count() FROM csv_result" 6

echo "--- JSONEachRow messages of the rows inserted by SQL"
$CLICKHOUSE_CLIENT -q "SELECT * FROM result ORDER BY id"

echo "--- CSV messages, with the key and the partition of the record"
$CLICKHOUSE_CLIENT -q "SELECT * FROM csv_result ORDER BY a"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE producer (id UInt64, name String, amount Decimal(10, 2), tags Array(String))
    ENGINE = Kafka
    SETTINGS kafka_broker_list = '${BROKER}', kafka_topic_list = '${CLICKHOUSE_DATABASE}.events', kafka_group_name = '${CLICKHOUSE_DATABASE}_producer',
        kafka_format = 'JSONEachRow';

    INSERT INTO producer SELECT 100 + number, 'produced', 1.5, ['x', 'y'] FROM numbers(3);
"

echo "--- the produced messages are parsed into the columns"
wait_for "SELECT count() FROM events" 13
$CLICKHOUSE_CLIENT -q "SELECT id, name, amount, tags, timestamp > '2026-01-01 00:00:00' FROM events WHERE id >= 100 ORDER BY id"

wait_for "SELECT count() FROM result" 13
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(id) FROM result WHERE id >= 100"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv;
    DROP TABLE consumer;
    DROP TABLE csv_mv;
    DROP TABLE csv_consumer;
    DROP TABLE producer;
    DROP TABLE result;
    DROP TABLE csv_result;
    DROP TABLE events;
    DROP TABLE csv_events;
"
