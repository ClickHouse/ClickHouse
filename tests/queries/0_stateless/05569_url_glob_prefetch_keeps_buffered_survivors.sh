#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `url` family generates its addresses in batches of 1000. To size the streams, batches are prefetched
# while the survivors of the `_path` / `_file` filter are fewer than the streams. When the next batch would
# cross the limit at once, the survivors already buffered are still read, and a query that stops at one of
# them succeeds. Here the first batch is the 1000 addresses `one` and `drop{1..999}`, the filter keeps only
# `one`, the next URL has two failover options (`|`), and the limit is 1001.
# The addresses are served by the HTTP interface of the server the test runs against: the path of a URL
# names a table, and the query in its parameters is what it returns.
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/system"
URL="${BASE}/one?query=SELECT+1,${BASE}/drop{1..999}?query=SELECT+2,${BASE}/{numbers|zeros}?query=SELECT+3"

# Parallel replicas may rewrite `url` into its cluster counterpart, which expands the pattern up front.
SETTINGS="glob_expansion_max_elements = 1001, enable_parallel_replicas = 0, parallelize_output_from_storages = 0"

for threads in 1 2 4
do
    $CLICKHOUSE_CLIENT --query "SELECT x FROM url('$URL', TSV, 'x UInt64') WHERE _file = 'one' LIMIT 1 SETTINGS $SETTINGS, max_threads = $threads"
done

# A query that reads past the survivors still gets the error.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') WHERE _file = 'one' SETTINGS $SETTINGS, max_threads = 2" 2>&1 \
    | grep -oF "too many result addresses: 1002, while at most 1001 are allowed" \
    | head -n 1
