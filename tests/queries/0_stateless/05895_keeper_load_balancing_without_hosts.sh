#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A [Zoo]Keeper section that sets a load balancing but lists no hosts must not kill the process when
# the client is created: `testkeeper` needs no hosts and has to work, and any other implementation has
# to fail with the ordinary "No hosts" error. Each case runs in its own `clickhouse-local`, so a signal
# death stays contained and cannot hide another case.

# `clickhouse-local` exits with a ClickHouse error code truncated to its low byte, so name the statuses
# that mean death by signal instead of testing the range.
signal_status()
{
    case "$1" in
        134) echo "died from signal 6" ;;
        136) echo "died from signal 8" ;;
        139) echo "died from signal 11" ;;
        *) echo "no signal death" ;;
    esac
}

# Runs the query against a config and prints the signal status; the output stays in "$out".
run_case()
{
    local name="$1" config_body="$2" query="$3"
    config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}.xml"
    out="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}.out"
    err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}.err"
    cat > "$config" <<EOF
<clickhouse>
    <keeper_map_path_prefix>/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}</keeper_map_path_prefix>
${config_body}
</clickhouse>
EOF
    ${CLICKHOUSE_LOCAL} --config-file="$config" \
        --path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}_d" --query "$query" > "$out" 2> "$err" < /dev/null
    signal_status "$?"
}

create_query="CREATE TABLE km (k String, v UInt64) ENGINE = KeeperMap('/km') PRIMARY KEY k;"

run_case testkeeper "
    <zookeeper>
        <implementation>testkeeper</implementation>
        <zookeeper_load_balancing>round_robin</zookeeper_load_balancing>
    </zookeeper>" "
    ${create_query}
    INSERT INTO km VALUES ('a', 1);
    SELECT 'testkeeper with load balancing', k, v FROM km;
"
cat "$out"

run_case zookeeper "
    <zookeeper>
        <zookeeper_load_balancing>random</zookeeper_load_balancing>
    </zookeeper>" "$create_query"
echo "zookeeper without nodes: $(grep -c -F 'No hosts passed to ZooKeeper constructor' "$err")"

run_case keeper_server "
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <keeper_load_balancing>random</keeper_load_balancing>
        <raft_configuration></raft_configuration>
    </keeper_server>" "$create_query"
echo "keeper_server without servers: $(grep -c -F 'No hosts passed to ZooKeeper constructor' "$err")"

for name in testkeeper zookeeper keeper_server; do
    rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}".{xml,out,err}
    rm -rf "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}_d"
done
