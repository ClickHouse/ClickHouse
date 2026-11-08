#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local must load SQL clusters from `--path` at startup. Without that, a second
# process sees no rows in `system.clusters` while `CREATE CLUSTER` still fails with
# `CLUSTER_ALREADY_EXISTS` because the metadata file is already there.

data_dir="${CLICKHOUSE_TMP}/sql_cluster_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"

cluster="c_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    CREATE CLUSTER ${cluster} (
        SHARD (host = '127.0.0.1', port = 9000)
    );
    SELECT count() FROM system.clusters WHERE cluster = '${cluster}' AND source = 'SQL';
"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    SELECT count() FROM system.clusters WHERE cluster = '${cluster}' AND source = 'SQL';
    DROP CLUSTER ${cluster};
    SELECT count() FROM system.clusters WHERE cluster = '${cluster}';
"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    SELECT count() FROM system.clusters WHERE cluster = '${cluster}';
"

rm -rf "$data_dir"
