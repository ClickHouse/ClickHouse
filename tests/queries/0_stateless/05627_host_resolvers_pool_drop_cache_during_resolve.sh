#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint pauses the first resolver created by any thread of the server.

# `SYSTEM CLEAR DNS CACHE` must not be undone by a resolution that started before it:
# a resolver created during the drop is used by its request, but is not cached.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Any address in 127.0.0.0/8 is local, so a random one is a host that no other request has resolved yet.
host="127.$((RANDOM % 250 + 1)).$((RANDOM % 250 + 1)).$((RANDOM % 250 + 1))"
query_id="${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT host_resolvers_pool_pause_before_insert"

$CLICKHOUSE_CLIENT --query_id "$query_id" -q "SELECT * FROM url('http://${host}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8')" &
pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT host_resolvers_pool_pause_before_insert PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR DNS CACHE"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT host_resolvers_pool_pause_before_insert"

wait "$pid"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND query_id = '$query_id'
        AND message = 'Not caching the resolver for host ${host}, because the cache was dropped during the resolution'"
