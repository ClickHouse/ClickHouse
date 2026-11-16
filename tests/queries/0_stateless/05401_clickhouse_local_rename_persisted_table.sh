#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local loads the persisted tables asynchronously, so `RENAME TABLE` as the first statement
# of a new process must wait for the tables it renames instead of treating them as nonexistent.
# A `Set` table with many entries takes a noticeable time to load.

data_dir="${CLICKHOUSE_TMP}/data_${CLICKHOUSE_DATABASE}"
rm -rf "$data_dir"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    CREATE TABLE t (x UInt64) ENGINE = Set;
    INSERT INTO t SELECT number FROM numbers(1000000);
"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    RENAME TABLE IF EXISTS t TO renamed;
    EXISTS TABLE renamed;
    EXISTS TABLE t;
"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "
    RENAME TABLE renamed TO t;
    EXISTS TABLE renamed;
    EXISTS TABLE t;
"

$CLICKHOUSE_LOCAL --path "$data_dir" --query "SELECT count() FROM numbers(10) WHERE number IN t"

rm -rf "$data_dir"
