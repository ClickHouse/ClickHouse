#!/usr/bin/env bash
# Tags: no-darwin
# Tag no-darwin: on Darwin symbols keep runtime addresses

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

# Address 0 resolves to nothing.
$CLICKHOUSE_CLIENT -q "SELECT addressToSymbol(0::UInt64) = '', addressToLine(0::UInt64) = '' SETTINGS allow_introspection_functions = 1"

# A libc symbol is not listed in system.symbols.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.symbols WHERE symbol = 'gnu_get_libc_version' SETTINGS allow_introspection_functions = 1"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT -q "CREATE USER IF NOT EXISTS $user"

# flameGraph requires the INTROSPECTION grant.
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT flameGraph([1::UInt64]) SETTINGS allow_introspection_functions = 1" 2>&1 | grep -o -m1 ACCESS_DENIED

# A stored flameGraph state also requires the grant when it is finalized.
$CLICKHOUSE_CLIENT -q "CREATE TABLE flame_graph_states (c AggregateFunction(flameGraph, Array(UInt64))) ENGINE = Memory SETTINGS allow_introspection_functions = 1"
echo "INSERT INTO flame_graph_states FORMAT Values ([1])" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&aggregate_function_input_format=value&allow_introspection_functions=1" --data-binary @-
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.flame_graph_states TO $user"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT finalizeAggregation(c) FROM flame_graph_states SETTINGS allow_introspection_functions = 1" 2>&1 | grep -o -m1 ACCESS_DENIED

$CLICKHOUSE_CLIENT -q "GRANT INTROSPECTION ON *.* TO $user"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT length(flameGraph([1::UInt64])) SETTINGS allow_introspection_functions = 1"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT length(finalizeAggregation(c)) FROM flame_graph_states SETTINGS allow_introspection_functions = 1"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"
