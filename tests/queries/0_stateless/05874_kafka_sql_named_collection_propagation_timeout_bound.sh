#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the Kafka engine is not built in fasttest
# no-replicated-database: named collections created by SQL are not replicated

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# librdkafka accepts `topic.metadata.propagation.max.ms` up to 3600000, but it converts the value to microseconds
# in a 32-bit integer, so the values above 2147483 overflow there. A named collection created by SQL cannot set
# such a value. The last value is 3600000 for librdkafka, which reads the numbers with `strtol`.
# No broker is needed: `CREATE TABLE` does not connect.

collection="${CLICKHOUSE_TEST_UNIQUE_NAME}"
values=('0' '1000' '2147483' '2147484' '3600000' '0x36ee80')

function cleanup()
{
    local queries=""
    local i
    for i in "${!values[@]}"
    do
        queries+=" DROP TABLE IF EXISTS kafka_table_${i}; DROP NAMED COLLECTION IF EXISTS ${collection}_${i};"
    done
    $CLICKHOUSE_CLIENT -q "$queries"
}
trap cleanup EXIT

for i in "${!values[@]}"
do
    if output=$($CLICKHOUSE_CLIENT -q "
        CREATE NAMED COLLECTION ${collection}_${i} AS
            kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
            \`kafka.topic_metadata_propagation_max_ms\` = '${values[$i]}';
        CREATE TABLE kafka_table_${i} (x UInt64) ENGINE = Kafka(${collection}_${i});" 2>&1)
    then
        echo "${values[$i]}: OK"
    else
        message=$(grep -o 'Kafka property .*(BAD_ARGUMENTS)' <<< "$output" | head -1)
        echo "${values[$i]}: ${message:-$output}"
    fi
done
