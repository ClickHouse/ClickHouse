#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint is server-wide, and another instance of the test could disable it too early.

# `SYSTEM CLEAR DNS CACHE` must not be undone by a resolution that started before it:
# a resolver created during the drop is used by its request, but is not cached.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Any address in 127.0.0.0/8 is local, so a random one is a host that no other request has resolved yet.
# The log message is matched by the host and not by `query_id`, because with parallel replicas
# the request is sent by a query of a replica.
host="127.$((RANDOM % 250 + 1)).$((RANDOM % 250 + 1)).$((RANDOM % 250 + 1))"

# The failpoint drops the cache in the thread that resolves the host, right after the resolution.
# It is deterministic, unlike pausing the resolution and running `SYSTEM CLEAR DNS CACHE` concurrently:
# a pause can be taken by a background request to another host.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT host_resolvers_pool_drop_cache_during_resolve"
$CLICKHOUSE_CLIENT -q "SELECT * FROM url('http://${host}:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8')"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT host_resolvers_pool_drop_cache_during_resolve"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count() > 0 FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
        AND message = 'Not caching the resolver for host ${host}, because the cache was dropped during the resolution'"
