#!/usr/bin/env bash

# `clickhouse-keeper` is a symlink to the `clickhouse` binary. It must keep supporting the subcommands
# of the former standalone `clickhouse-keeper` binary: `client`, `keeper-client`, `converter`,
# `keeper-converter`, `keeper` (also with the `--` prefix), and run the Keeper server otherwise.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$dir"
trap 'rm -rf "$dir"' EXIT
ln -sf "$(realpath "$CLICKHOUSE_BINARY")" "$dir/clickhouse-keeper"
keeper="$dir/clickhouse-keeper"

# Prints which tool handled the invocation, judging by its help output.
function which_tool()
{
    local output
    output=$("$keeper" "$@" --help 2>&1)
    if grep -q -F 'keeper-client-history' <<< "$output"; then
        echo "$* -> keeper-client"
    elif grep -q -F 'zookeeper-logs-dir' <<< "$output"; then
        echo "$* -> keeper-converter"
    elif grep -q -F 'clickhouse-keeper [OPTION] [-- [ARG]...]' <<< "$output"; then
        echo "$* -> keeper"
    else
        echo "$* -> unknown"
    fi
}

which_tool client
which_tool --client
which_tool keeper-client
which_tool converter
which_tool keeper-converter
which_tool --keeper-converter
which_tool keeper
which_tool
