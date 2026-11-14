#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The limit on a pattern that generates shards (`,`) and replicas (`|`) at the same time is on the
# addresses of both stages together, and the addresses a `_path` / `_file` filter rejects count too:
# `{a,b,c}{1|2}` is six addresses even when only the shard `c` is read. The limit is reported before
# any of them is requested, so the hosts do not have to exist.
# Parallel replicas may rewrite `url` into its cluster counterpart, which changes the surface, so
# pin the plain code path.

URL="http://localhost:1/{a,b,c}{1|2}.tsv"

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 3 --query "SELECT count() FROM url('$URL', TSV, 'x UInt64') WHERE _file LIKE 'c%' SETTINGS enable_parallel_replicas = 0, max_threads = 1" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 6, while at most 3 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.url_filtered_mixed_glob (x UInt64) ENGINE = URL('$URL', TSV)"
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 3 --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.url_filtered_mixed_glob WHERE _file LIKE 'c%' SETTINGS enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table engine 'URL'" -e "too many result addresses: 6, while at most 3 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 3 --query "SELECT count() FROM urlCluster('test_shard_localhost', '$URL', TSV, 'x UInt64') WHERE _file LIKE 'c%'" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 6, while at most 3 are allowed" \
    | head -n 2
