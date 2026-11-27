#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-keeper-client` runs a positional argument as the query, like `clickhouse-local`.

echo "-- positional query"
$CLICKHOUSE_KEEPER_CLIENT "exists /"; echo "rc=$?"

echo "-- positional query together with --query"
$CLICKHOUSE_KEEPER_CLIENT -q "exists /" "exists /" 2>&1; echo "rc=$?"

echo "-- two positional queries"
$CLICKHOUSE_KEEPER_CLIENT "exists /" "ls /" 2>&1; echo "rc=$?"
