#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A full-definition ATTACH gets the table name length checked like CREATE.

DB="${CLICKHOUSE_DATABASE}_$(printf 'x%.0s' {1..200})"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB} ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.ta (x Int) ENGINE = MergeTree ORDER BY x; -- { serverError ARGUMENT_OUT_OF_BOUND }"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${DB}.ta UUID '$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")' (x Int) ENGINE = MergeTree ORDER BY x; -- { serverError ARGUMENT_OUT_OF_BOUND }"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${DB}'"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB}"
