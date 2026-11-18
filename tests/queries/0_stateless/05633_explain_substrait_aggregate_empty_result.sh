#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Substrait is not built in fast test

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Over empty input both queries return no rows in ClickHouse. A Substrait aggregate without keys would return one row.
# Of each exception only the message is printed.
$CLICKHOUSE_CLIENT --enable_substrait 1 --ignore-error -q "
    DROP TABLE IF EXISTS t_substrait_empty_result;
    CREATE TABLE t_substrait_empty_result (x Int32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_substrait_empty_result VALUES (1);

    EXPLAIN SUBSTRAIT SELECT count() FROM t_substrait_empty_result GROUP BY 'a';

    SET empty_result_for_aggregation_by_empty_set = 1;
    EXPLAIN SUBSTRAIT SELECT count() FROM t_substrait_empty_result;

    DROP TABLE t_substrait_empty_result;
" 2>&1 | sed -E '/^Received exception/d; /^\(query:/d; s/^Code: .*DB::Exception: //; s/\. \(NOT_IMPLEMENTED\)$//'
