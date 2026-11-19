#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local loads the persisted tables asynchronously. If the startup fails after the load
# was scheduled (here: `--database` names a nonexistent database), the load must be stopped before
# the context is shut down and the lock of the directory is released.
# A `Set` table with many entries takes a noticeable time to load.

data_dir="${CLICKHOUSE_TMP}/data_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    CREATE TABLE t (x UInt64) ENGINE = Set;
    INSERT INTO t SELECT number FROM numbers(1000000);
    CREATE TABLE m (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO m SELECT number FROM numbers(1000);
"

$CLICKHOUSE_LOCAL --path "$data_dir" --database does_not_exist --query "SELECT 1" 2>&1 | grep -o -m1 'UNKNOWN_DATABASE'

$CLICKHOUSE_LOCAL --path "$data_dir" --query "SELECT count() FROM numbers(10) WHERE number IN t; SELECT count() FROM m"

rm -rf "$data_dir"
