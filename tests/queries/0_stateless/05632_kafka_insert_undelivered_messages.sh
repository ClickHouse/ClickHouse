#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-async-insert
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: named collections created by SQL are not replicated
# Tag no-async-insert: the KILL case needs the INSERT itself to run the producer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An INSERT into a Kafka table must fail when its messages are not delivered, and must be killable while it waits.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
missing_topic=$(echo "${name}_missing" | tr '_' '-')
insert_id="${name}_insert"

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    for table in missing_topic no_broker no_broker_keeper kill; do
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${name}_${table}" 2>/dev/null
    done
    for collection in missing_topic no_broker kill; do
        $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${name}_${collection}" 2>/dev/null
    done

    exit $exit_code
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# The CI broker on 127.0.0.1:9092 does not create topics automatically.
$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${name}_missing_topic AS
        kafka_broker_list = '127.0.0.1:9092', kafka_topic_list = '${missing_topic}', kafka_group_name = '${name}', kafka_format = 'JSONEachRow',
        \`kafka.allow_auto_create_topics\` = 'false', \`kafka.topic_metadata_propagation_max_ms\` = '1000', \`kafka.message_timeout_ms\` = '30000';
    CREATE NAMED COLLECTION ${name}_no_broker AS
        kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'topic', kafka_group_name = '${name}', kafka_format = 'JSONEachRow',
        \`kafka.message_timeout_ms\` = '1000';
    CREATE NAMED COLLECTION ${name}_kill AS
        kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'topic', kafka_group_name = '${name}', kafka_format = 'JSONEachRow',
        \`kafka.message_timeout_ms\` = '30000', \`kafka.queue_buffering_max_messages\` = '1';
    CREATE TABLE ${name}_missing_topic (x UInt64) ENGINE = Kafka(${name}_missing_topic);
    CREATE TABLE ${name}_no_broker (x UInt64) ENGINE = Kafka(${name}_no_broker);
    CREATE TABLE ${name}_kill (x UInt64) ENGINE = Kafka(${name}_kill);"
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${name}_no_broker_keeper (x UInt64) ENGINE = Kafka(${name}_no_broker)
        SETTINGS kafka_keeper_path = '/clickhouse/test/${name}', kafka_replica_name = 'r1'"

echo "missing topic: $($CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_missing_topic VALUES (1)" 2>&1 | grep -m1 -c "Failed to deliver 1 message(s) to Kafka topic '${missing_topic}'.*Unknown topic")"
echo "no broker: $($CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_no_broker VALUES (1)" 2>&1 | grep -m1 -c 'Failed to deliver 1 message(s).*Message timed out')"
echo "no broker, offsets in Keeper: $($CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_no_broker_keeper VALUES (1)" 2>&1 | grep -m1 -c 'Failed to deliver 1 message(s).*Message timed out')"

# Message 1 waits for the broker, message 2 waits for room in the one-message queue.
$CLICKHOUSE_CLIENT --query_id "$insert_id" -q "INSERT INTO ${name}_kill VALUES (1), (2)" > "${CLICKHOUSE_TMP}/${name}_kill.out" 2>&1 &
for _ in {1..300}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['KafkaMessagesProduced'] FROM system.processes WHERE query_id = '$insert_id'")" = 1 ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$insert_id' ASYNC FORMAT Null"
for _ in {1..150}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$insert_id'")" = 0 ] && break
    sleep 0.1
done
echo "killed: $($CLICKHOUSE_CLIENT -q "SELECT count() = 0 FROM system.processes WHERE query_id = '$insert_id'")"
wait
echo "cancelled: $(grep -m1 -c QUERY_WAS_CANCELLED "${CLICKHOUSE_TMP}/${name}_kill.out")"
