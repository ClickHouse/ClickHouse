#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# The PromQL sort functions are applied as the ORDER BY of the SQL query generated for an instant query, and only when
# they are the outermost function. The test prints that ORDER BY, taken from the debug log of `prometheusQuery`:
# `sort` and `sort_desc` order by value, `sort_by_label` and `sort_by_label_desc` order by one natural sort key for
# each non-empty label name, and in every case the tags are the last key.
#
# The exact text of the ORDER BY is compared on purpose: the other tests of the sort functions check only the order of
# the result, which a wrong key can still produce on their data. A change of the key has to update the reference here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The generated SQL is logged at the debug level.
client=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=debug"}

# The table stays empty: the SQL is generated and logged without data.
$CLICKHOUSE_CLIENT --enable_time_series_table 1 --query "CREATE TABLE ts ENGINE = TimeSeries"

# Prints the ORDER BY of the SQL generated for the specified PromQL query.
function order_by()
{
    local name=$1
    local promql=$2

    echo "-- ${name}"

    # The logged query takes several lines and ends where the next log message starts, with `[`.
    # The ORDER BY of the outermost SELECT is the last one in the query. Each of its keys is printed on its own line.
    $client --enable_time_series_table 1 --query "SELECT * FROM prometheusQuery(ts, '${promql}', 3000) FORMAT Null" 2>&1 \
        | awk '/Will execute query/ { in_query = 1; next } in_query && /^\[/ { exit } in_query { print }' \
        | tr -s '\n ' ' ' \
        | sed -E 's/.*(ORDER BY )/\1/; s/ +$//; s/ (ASC|DESC), / \1,\n    /g'
    echo
}

order_by "by value" 'sort_desc(up)'
order_by "by labels" 'sort_by_label(up, "instance", "", "job")'
order_by "not the root" 'abs(sort_by_label(up, "instance"))'

$CLICKHOUSE_CLIENT --query "DROP TABLE ts"
