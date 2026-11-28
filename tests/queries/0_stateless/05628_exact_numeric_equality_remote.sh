#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_exact_numeric_equality_remote"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_exact_numeric_equality_remote (d Decimal(18, 0), x Float64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_exact_numeric_equality_remote VALUES ('9007199254740993', 9007199254740992.)"

${CLICKHOUSE_CLIENT} --allow_lossy_numeric_supertype=1 --query "SELECT count() FROM t_exact_numeric_equality_remote WHERE d IN (materialize(x))"
${CLICKHOUSE_CLIENT} --allow_lossy_numeric_supertype=1 --query "SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_exact_numeric_equality_remote') WHERE d IN (materialize(x)) SETTINGS prefer_localhost_replica = 0"
${CLICKHOUSE_CLIENT} --allow_lossy_numeric_supertype=1 --query "SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't_exact_numeric_equality_remote') WHERE d IN (materialize(x)) SETTINGS prefer_localhost_replica = 0, make_distributed_plan = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_exact_numeric_equality_remote"
