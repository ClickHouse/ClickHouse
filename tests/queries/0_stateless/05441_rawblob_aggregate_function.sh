#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# `RawBLOB` must write the serialized state of an aggregate function, the same as its conversion to `String`,
# not the pointer to the state in memory.

$CLICKHOUSE_CLIENT -q "SELECT sumState(number) FROM numbers(3) FORMAT RawBLOB" | od -An -tx1 | tr -d ' \n'
echo

$CLICKHOUSE_CLIENT -q "SELECT quantileState(number) FROM numbers(100000) FORMAT RawBLOB" | md5sum
$CLICKHOUSE_CLIENT -q "SELECT CAST(quantileState(number) AS String) FROM numbers(100000) FORMAT RawBLOB" | md5sum

$CLICKHOUSE_CLIENT -q "SELECT uniqExactState(number) FROM numbers(10) GROUP BY number % 2 ORDER BY number % 2 FORMAT RawBLOB" | md5sum
$CLICKHOUSE_CLIENT -q "SELECT CAST(uniqExactState(number) AS String) FROM numbers(10) GROUP BY number % 2 ORDER BY number % 2 FORMAT RawBLOB" | md5sum
