#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# The effective settings folded into a view's modification hash must be framed: without the lengths,
# `{format_schema = 'xjoin_use_nulls1'}` and `{format_schema = 'x', join_use_nulls = 1}` feed the same
# bytes into the hash, although `join_use_nulls` changes the rows of a `LEFT JOIN`.

$CLICKHOUSE_CLIENT -q "
    DROP VIEW IF EXISTS v_framing_05406;
    DROP TABLE IF EXISTS t_framing_05406;
    CREATE TABLE t_framing_05406 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_framing_05406 VALUES (1);
    CREATE VIEW v_framing_05406 AS SELECT x FROM t_framing_05406;
"

hash_of_view()
{
    $CLICKHOUSE_CLIENT "$@" -q "
        SELECT toString(modification_hash)
        FROM system.tables
        WHERE database = currentDatabase() AND name = 'v_framing_05406'
    "
}

concatenated=$(hash_of_view --format_schema 'xjoin_use_nulls1')
separate=$(hash_of_view --format_schema 'x' --join_use_nulls 1)

[ -n "${concatenated}" ] && [ -n "${separate}" ] && echo 'hashes are computed'
[ "${concatenated}" != "${separate}" ] && echo 'differently split settings hash differently'

$CLICKHOUSE_CLIENT -q "
    DROP VIEW v_framing_05406;
    DROP TABLE t_framing_05406;
"
