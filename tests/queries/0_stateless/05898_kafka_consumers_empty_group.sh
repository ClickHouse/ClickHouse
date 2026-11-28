#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: the Kafka2 table uses a fixed replica name in Keeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A consumer with an empty `kafka_group_name` made every read of `system.kafka_consumers` fail with `Local: Unknown group`.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
topic=$(echo "${name}" | tr '_' '-')
broker="127.0.0.1:9092"

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    for table in mv mv2 kafka kafka2 dst; do
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${name}_${table}" 2>/dev/null
    done
    timeout 10 rpk topic delete "$topic" --brokers "$broker" > /dev/null 2>&1

    exit $exit_code
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

rpk topic create "$topic" -p 1 --brokers "$broker" > /dev/null 2>&1

$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${name}_dst (id UInt64) ENGINE = Null;
    CREATE TABLE ${name}_kafka (id UInt64) ENGINE = Kafka('${broker}', '${topic}', '') SETTINGS kafka_format = 'TSV';
    CREATE TABLE ${name}_kafka2 (id UInt64) ENGINE = Kafka('${broker}', '${topic}', '')
        SETTINGS kafka_format = 'TSV', kafka_keeper_path = '/clickhouse/test/${name}', kafka_replica_name = 'r1';
    CREATE MATERIALIZED VIEW ${name}_mv TO ${name}_dst AS SELECT id FROM ${name}_kafka;
    CREATE MATERIALIZED VIEW ${name}_mv2 TO ${name}_dst AS SELECT id FROM ${name}_kafka2;"

# Wait until the Kafka consumer failed to subscribe and the Kafka2 consumer was used once.
query="
    SELECT table, if(table LIKE '%kafka2', toUnixTimestamp64Micro(last_used) > 0, arrayExists(x -> x LIKE '%Unknown group%', exceptions.text)) AS ready
    FROM system.kafka_consumers WHERE database = currentDatabase() ORDER BY table"
for _ in {1..600}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT countIf(ready) FROM (${query})" 2>/dev/null)" = 2 ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "${query}" | sed "s/${name}_//"
