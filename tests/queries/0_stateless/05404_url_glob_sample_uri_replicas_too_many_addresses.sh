#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The first address of a `url` pattern is generated when the table is created, to read the hive
# partitioning and the virtual columns off its path. A group with a comma inside is expanded eagerly
# at that point, and the reported number has to cover the replicas of every address as well:
# `{1,2,3}{1|2}` describes 6 addresses, not 3.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM url('${CLICKHOUSE_URL}&query=SELECT+{1,2,3}{1|2}', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 2, enable_parallel_replicas = 0" 2>&1 \
    | grep -oF -e "Table function 'url'" -e "too many result addresses: 6, while at most 2 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --query "SELECT count() FROM urlCluster('test_shard_localhost', '${CLICKHOUSE_URL}&query=SELECT+{1,2,3}{1|2}', TSV, 'x UInt64') SETTINGS glob_expansion_max_elements = 2" 2>&1 \
    | grep -oF -e "Table function 'urlCluster'" -e "too many result addresses: 6, while at most 2 are allowed" \
    | head -n 2
