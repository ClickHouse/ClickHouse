#!/usr/bin/env bash
# Tags: shard, no-ordinary-database
# no-ordinary-database: `RENAME` waits for the running query.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `skip_unavailable_shards` skips a shard whose table disappears after the connection was established.
# After the first rows the dictionary looks `v` up for every row, so the shard fails as soon as `v` is renamed.

$CLICKHOUSE_CLIENT -q "
    CREATE VIEW v AS SELECT number AS x FROM system.numbers;
    CREATE VIEW healthy AS SELECT number AS x FROM system.numbers;
    CREATE DICTIONARY dict (x UInt64) PRIMARY KEY x LAYOUT(DIRECT())
    SOURCE(CLICKHOUSE(QUERY 'SELECT x FROM (SELECT x FROM $CLICKHOUSE_DATABASE.v LIMIT 1)'))"

function from()
{
    echo "cluster(test_shard_localhost, $CLICKHOUSE_DATABASE.$1) WHERE NOT sleepEachRow(0.05) AND (x < 10 OR dictHas('$CLICKHOUSE_DATABASE.dict', x - x))"
}

prefix="${CLICKHOUSE_DATABASE}_$RANDOM"

function run()
{
    echo "$1: $($CLICKHOUSE_CLIENT --query_id "${prefix}_$1" -q "$2
        SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 0, max_block_size = 1, max_execution_time = 60,
            short_circuit_function_evaluation = 'force_enable', enable_parallel_replicas = 0, serialize_query_plan = 0" 2>&1 \
        | grep -m1 -oE '^[0-9]+$|ALL_CONNECTION_TRIES_FAILED|UNKNOWN_TABLE')"
}

{
    run another_table "SELECT count() FROM $(from healthy)" &
    # The only shard is skipped.
    run no_rows "SELECT count() FROM $(from v)" &
    # A shard that streamed rows and only then was skipped has answered.
    run some_rows "SELECT count() > 0 FROM (SELECT x FROM $(from v))" &

    for _ in {1..300}
    do
        [[ $($CLICKHOUSE_CLIENT -q "
            SELECT count() FROM system.processes WHERE startsWith(initial_query_id, '$prefix') AND NOT is_initial_query AND read_rows > 0") == 3 ]] && break
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "RENAME TABLE v TO renamed SETTINGS check_table_dependencies = 0, check_referential_table_dependencies = 0"
    wait
} | sort

$CLICKHOUSE_CLIENT -q "DROP DICTIONARY dict"
