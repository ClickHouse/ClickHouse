#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: named collections created by SQL are not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An INSERT into a Kafka table must fail, not succeed with rows missing, when DROP or DETACH stops it.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
topic=$(echo "${name}" | tr '_' '-')
broker="127.0.0.1:9092"

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    for table in drop detach keeper; do
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${name}_${table}" 2>/dev/null
    done
    $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${name}" 2>/dev/null
    timeout 10 rpk topic delete "$topic" --brokers "$broker" > /dev/null 2>&1

    exit $exit_code
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

rpk topic create "$topic" -p 1 --brokers "$broker" > /dev/null 2>&1

# Message 1 lingers in the one-message queue, so message 2 waits for room until the table is shut down.
$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${name} AS
        kafka_broker_list = '${broker}', kafka_topic_list = '${topic}', kafka_group_name = '${name}', kafka_format = 'JSONEachRow',
        \`kafka.queue_buffering_max_messages\` = '1', \`kafka.linger_ms\` = '60000'"

# $1: case name, $2: statement that shuts the table down, $3: extra CREATE clause, $4: extra client settings for CREATE
insert_and_shut_down()
{
    local table="${name}_$1"
    local query_id="${name}_$1_insert"

    $CLICKHOUSE_CLIENT $4 -q "CREATE TABLE ${table} (x UInt64) ENGINE = Kafka(${name}) $3"
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "INSERT INTO ${table} SETTINGS async_insert = 0 VALUES (1), (2)" > "${CLICKHOUSE_TMP}/${table}.out" 2>&1 &
    for _ in {1..300}; do
        [ "$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['KafkaMessagesProduced'] FROM system.processes WHERE query_id = '$query_id'")" = 1 ] && break
        sleep 0.1
    done
    $CLICKHOUSE_CLIENT -q "$2 TABLE ${table}"
    wait
    echo "$1: $(grep -m1 -c 'ABORTED' "${CLICKHOUSE_TMP}/${table}.out")"
}

insert_and_shut_down drop DROP
insert_and_shut_down detach DETACH
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${name}_detach"
insert_and_shut_down keeper DROP "SETTINGS kafka_keeper_path = '/clickhouse/test/${name}', kafka_replica_name = 'r1'" "--allow_kafka_offsets_storage_in_keeper 1"
