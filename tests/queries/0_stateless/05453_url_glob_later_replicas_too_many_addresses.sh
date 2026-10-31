#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `url` family generates its addresses lazily, so a query that stops early never reads past the
# limit. This holds when the address that crosses the limit is a later one whose failover options (`|`)
# alone exceed it: the addresses before it are still read, and only a query that asks for more than
# them gets the error. Here the first address is one, the second one has three failover options, and
# the limit is two. The addresses are served by the HTTP interface of the server the test runs against.
URL="${CLICKHOUSE_URL}&query=SELECT+1,${CLICKHOUSE_URL}&query=SELECT+{2|3|4}"

# Parallel replicas may rewrite `url` into its cluster counterpart, which expands the pattern up front.
SETTINGS="glob_expansion_max_elements = 2, max_threads = 1, enable_parallel_replicas = 0"

$CLICKHOUSE_CLIENT --query "SELECT * FROM url('$URL', TSV, 'x UInt64') LIMIT 1 SETTINGS $SETTINGS"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') SETTINGS $SETTINGS" 2>&1 \
    | grep -oF "too many result addresses: 4, while at most 2 are allowed" \
    | head -n 1
