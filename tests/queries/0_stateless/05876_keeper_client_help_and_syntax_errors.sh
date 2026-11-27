#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Usability of `clickhouse-keeper-client`: `help <command>`, syntax errors which say what is missing,
# and the `--help` output. None of the commands below modify anything in Keeper.

echo "-- help for a command"
$CLICKHOUSE_KEEPER_CLIENT -q "help create"

echo "-- help for an unknown command"
$CLICKHOUSE_KEEPER_CLIENT -q "help no_such_command" 2>&1

echo "-- help for a four-letter-word command"
$CLICKHOUSE_KEEPER_CLIENT -q "help ruok"

echo "-- help without arguments lists all commands"
$CLICKHOUSE_KEEPER_CLIENT -q "help" | grep -F -e 'help [command]' -e 'create <path>'

echo "-- missing value"
$CLICKHOUSE_KEEPER_CLIENT -q "create /x" 2>&1
$CLICKHOUSE_KEEPER_CLIENT -q "set /x" 2>&1

echo "-- missing path"
$CLICKHOUSE_KEEPER_CLIENT -q "create" 2>&1

echo "-- missing watch id"
$CLICKHOUSE_KEEPER_CLIENT -q "watch" 2>&1

echo "-- misspelled optional argument"
$CLICKHOUSE_KEEPER_CLIENT -q "create /x v EPHEMRAL" 2>&1

echo "-- unknown command"
$CLICKHOUSE_KEEPER_CLIENT -q "no_such_command /x" 2>&1

echo "-- --help"
$CLICKHOUSE_KEEPER_CLIENT --help | grep -o -e '^Usage: clickhouse' -e 'keeper-client \[OPTION\] \[query\]' -e 'non-interactive'
