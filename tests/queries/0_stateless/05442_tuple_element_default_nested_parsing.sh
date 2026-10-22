#!/usr/bin/env bash

# Regression test: parsing a valid `Tuple` that nests an element `DEFAULT` at every level must take
# linear time. Switching from the `Tuple` fast path to the generic representation used to rewind and
# reparse the whole argument list, so every nested type was parsed twice per level - 2^N in total,
# exceeding `max_parser_backtracks` with `TOO_SLOW_PARSING` at depth 19.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

type='UInt8'
for i in $(seq 0 39); do
    type="Tuple(n${i} ${type}, d${i} UInt8 DEFAULT 1)"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_tuple_default_nested_parsing"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_tuple_default_nested_parsing (id UInt8, c ${type}) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_tuple_default_nested_parsing (id) VALUES (1)"
$CLICKHOUSE_CLIENT -q "SELECT c FROM t_tuple_default_nested_parsing"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_tuple_default_nested_parsing"
