#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `url` family generates its addresses lazily, so a query that stops early never reads past the
# limit. Without a `_path` / `_file` filter the number of streams must still be sized by the addresses
# that fit into the limit, not by the number of URLs: here the first URL has two failover options (`|`),
# so it alone takes the whole limit of two, and a second stream would ask for the second URL past the
# limit while the first stream already reads the first one. A predicate on a physical column keeps the
# query from being reduced to one stream by its `LIMIT`. The addresses are served by the HTTP interface
# of the server the test runs against.
URL="${CLICKHOUSE_URL}&query=SELECT+{1|1},${CLICKHOUSE_URL}&query=SELECT+1"

# Parallel replicas may rewrite `url` into its cluster counterpart, which expands the pattern up front.
SETTINGS="glob_expansion_max_elements = 2, max_threads = 2, enable_parallel_replicas = 0, parallelize_output_from_storages = 0"

$CLICKHOUSE_CLIENT --query "SELECT x FROM url('$URL', TSV, 'x UInt64') WHERE x = 1 LIMIT 1 SETTINGS $SETTINGS"

# Reading everything still reports the limit.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') WHERE x = 1 SETTINGS $SETTINGS" 2>&1 \
    | grep -oF "too many result addresses: 3, while at most 2 are allowed" \
    | head -n 1
