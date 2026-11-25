#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-replicated-database
# no-replicated-database: that job has no Keeper max_request_size, and the test needs one.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

value() { echo "substring(repeat(repeat('a', 1000), 12000), 1, $1)"; }

$CLICKHOUSE_CLIENT -q "CREATE TABLE kv (key String, value String) ENGINE = KeeperMap('/' || currentDatabase() || '/kv') PRIMARY KEY key"

# An insert over the limit is rejected before it is sent, with its exact size and the limit in the message.
probe_len=11000000
probe=$($CLICKHOUSE_CLIENT -q "INSERT INTO kv SELECT 'k', $(value $probe_len)" 2>&1)
size=$(grep -oE 'Request size [0-9]+' <<< "$probe" | head -1 | grep -oE '[0-9]+')
limit=$(grep -oE 'exceeds limit [0-9]+' <<< "$probe" | head -1 | grep -oE '[0-9]+')
if [[ -z "$size" || -z "$limit" ]]; then echo "probe not rejected by the size limit: $probe"; exit 1; fi

# Exactly at the limit: only the send-time check, which also counts the length prefix, rejects it.
$CLICKHOUSE_CLIENT -q "INSERT INTO kv SELECT 'k', $(value $((probe_len - size + limit)))" 2>&1 | grep -oE -m1 'KEEPER_EXCEPTION|LOGICAL_ERROR'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM kv"
$CLICKHOUSE_CLIENT -q "DROP TABLE kv"
