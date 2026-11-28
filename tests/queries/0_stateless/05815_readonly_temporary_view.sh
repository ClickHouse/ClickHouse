#!/usr/bin/env bash
# `CREATE TEMPORARY VIEW` must be refused with `readonly = 1`, the same as `CREATE TEMPORARY TABLE`,
# and allowed with `readonly = 2`.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --readonly 1 --query "SELECT 'select is allowed'"
$CLICKHOUSE_CLIENT --readonly 1 --query "CREATE TEMPORARY TABLE tt (x UInt8)" 2>&1 | grep -o -m1 'Cannot execute query in readonly mode'
$CLICKHOUSE_CLIENT --readonly 1 --query "CREATE TEMPORARY VIEW tv AS SELECT 1 AS x" 2>&1 | grep -o -m1 'Cannot execute query in readonly mode'
$CLICKHOUSE_CLIENT --readonly 2 --query "CREATE TEMPORARY TABLE tt (x UInt8); SELECT count() FROM tt"
$CLICKHOUSE_CLIENT --readonly 2 --query "CREATE TEMPORARY VIEW tv AS SELECT 3 AS x; SELECT x FROM tv"
