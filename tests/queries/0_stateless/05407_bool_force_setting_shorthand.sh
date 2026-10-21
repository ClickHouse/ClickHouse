#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SET` shorthand changes both `false` and `force` to `true`.
$CLICKHOUSE_CLIENT --multiquery --query "
    SET query_plan_read_in_order_through_join = 0;
    SET query_plan_read_in_order_through_join;
    SELECT value FROM system.settings WHERE name = 'query_plan_read_in_order_through_join';
    SET query_plan_read_in_order_through_join = 'force';
    SET query_plan_read_in_order_through_join;
    SELECT value FROM system.settings WHERE name = 'query_plan_read_in_order_through_join';
"

# Query-level `SETTINGS` shorthand overrides a disabled session setting.
$CLICKHOUSE_CLIENT --query_plan_read_in_order_through_join=0 --query "
    SELECT value FROM system.settings WHERE name = 'query_plan_read_in_order_through_join'
    SETTINGS query_plan_read_in_order_through_join
"

# A client option without a value means `true`.
$CLICKHOUSE_CLIENT --query_plan_read_in_order_through_join --query "
    SELECT value FROM system.settings WHERE name = 'query_plan_read_in_order_through_join'
"
