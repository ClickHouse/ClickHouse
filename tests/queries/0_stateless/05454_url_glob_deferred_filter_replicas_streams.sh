#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `_path` / `_file` filter whose sets are not ready yet is applied only when the first stream asks for
# an address. Until then, the number of streams is taken from the whole pattern only when it is known to
# fit into the limit - which the number of URLs alone does not tell when some of them have failover
# options (`|`): here the URLs are three, but the addresses are four, and the limit is three. A second
# stream would find nothing left after the filter keeps only the first URL and would ask past the limit
# while the first one is still reading. The failpoint defers the filter deterministically.
# The addresses are served by the HTTP interface of the server the test runs against: the path of a URL
# names a table, and the query in its parameters is what it returns.
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/system"
URL="${BASE}/one?query=SELECT+1,${BASE}/{numbers|zeros}?query=SELECT+2,${BASE}/tables?query=SELECT+3"

# Parallel replicas may rewrite `url` into its cluster counterpart, which expands the pattern up front.
SETTINGS="glob_expansion_max_elements = 3, max_threads = 2, enable_parallel_replicas = 0, parallelize_output_from_storages = 0"

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT url_glob_defer_path_filter;
    SELECT x FROM url('$URL', TSV, 'x UInt64') WHERE _file = 'one' LIMIT 1 SETTINGS $SETTINGS;
"

# Without the failpoint the filter is applied up front, and the result is the same.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT url_glob_defer_path_filter"
$CLICKHOUSE_CLIENT --query "SELECT x FROM url('$URL', TSV, 'x UInt64') WHERE _file = 'one' LIMIT 1 SETTINGS $SETTINGS"
