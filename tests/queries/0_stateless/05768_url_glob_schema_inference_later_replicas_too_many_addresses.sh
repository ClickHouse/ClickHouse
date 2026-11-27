#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Schema inference generates the addresses lazily as well and stops at the first one it can read, so
# a later address whose failover options (`|`) alone exceed the limit must not fail it before the
# addresses before it are tried. Here the first address is one, the second one has three failover
# options, and the limit is two. The structure is not given, so it is inferred from the first address.
URL="${CLICKHOUSE_URL}&query=SELECT+1,${CLICKHOUSE_URL}&query=SELECT+{2|3|4}"

# Parallel replicas may rewrite `url` into its cluster counterpart, which expands the pattern up front.
# The query string of the URL looks like a hive partition, which would add a column to the inferred structure.
SETTINGS="glob_expansion_max_elements = 2, max_threads = 1, enable_parallel_replicas = 0, schema_inference_use_cache_for_url = 0, use_hive_partitioning = 0"

$CLICKHOUSE_CLIENT --query "DESCRIBE url('$URL', TSV) SETTINGS $SETTINGS"
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('$URL', TSV) LIMIT 1 SETTINGS $SETTINGS"

# Reading everything still reports the limit.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('$URL', TSV) SETTINGS $SETTINGS" 2>&1 \
    | grep -oF "too many result addresses: 4, while at most 2 are allowed" \
    | head -n 1
