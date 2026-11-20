#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Substrait is not built in fast test

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --enable_substrait 1 --query_plan_lower_array_join_function 0 --ignore-error -q "
    DROP TABLE IF EXISTS t_substrait_array_join;
    CREATE TABLE t_substrait_array_join (x Int64, a Array(Int64)) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_substrait_array_join VALUES (1, [10, 20]);

    EXPLAIN SUBSTRAIT SELECT x FROM (SELECT x, arrayJoin(a) AS unused FROM t_substrait_array_join);

    DROP TABLE t_substrait_array_join;
" 2>&1 | sed -E '/^Received exception/d; /^\(query:/d; s/^Code: .*DB::Exception: //; s/\. \(NOT_IMPLEMENTED\)$//'
