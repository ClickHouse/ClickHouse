#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The name is unique per run, so the test can run in parallel with itself.
user="user_05427_$CLICKHOUSE_DATABASE"
T=1700000000

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --session_timezone=UTC"
TENANT="$CLIENT --user $user"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $CLICKHOUSE_DATABASE.* TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TEMPORARY TABLE ON *.* TO $user"

function fill()
{
    $CLIENT -q "
        INSERT INTO $1 (metric_name, tags, samples) VALUES
            ('up', {'tenant': 'a', 'instance': 'a1'}, [(toDateTime64($T - 60, 3), 1), (toDateTime64($T, 3), 1)]),
            ('up', {'tenant': 'a', 'instance': 'a2'}, [(toDateTime64($T - 60, 3), 1), (toDateTime64($T, 3), 0)]),
            ('up', {'tenant': 'b', 'instance': 'b1'}, [(toDateTime64($T - 60, 3), 1), (toDateTime64($T, 3), 1)]),
            ('secret', {'tenant': 'b', 'secret_only': 'yes'}, [(toDateTime64($T - 60, 3), 42), (toDateTime64($T, 3), 43)]),
            ('untagged', {'instance': 'z1'}, [(toDateTime64($T - 60, 3), 7), (toDateTime64($T, 3), 7)]);
        INSERT INTO $1 (metric_family, type, unit, help) VALUES ('up', 'gauge', '', 'up'), ('secret', 'gauge', '', 'secret'), ('untagged', 'gauge', '', 'untagged');
    "
}

function check_table()
{
    local table=$1
    local tags_table
    tags_table=$($CLIENT -q "SELECT '.inner_id.tags.' || toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = '$table'")

    $CLIENT -q "CREATE ROW POLICY tenant_a ON $table FOR SELECT USING tags['tenant'] = 'a' TO $user"
    $CLIENT -q "CREATE ROW POLICY everything ON $table FOR SELECT USING 1 TO ALL EXCEPT $user"

    echo "--- $table: the tenant reads only its own series ---"
    $TENANT -q "SELECT metric_name, tags FROM $table ORDER BY metric_name, tags"
    echo "-- timeSeriesTags"
    $TENANT -q "SELECT metric_name, tags FROM timeSeriesTags($table) ORDER BY metric_name, tags"
    echo "-- inner tags table"
    $TENANT -q "SELECT metric_name, tags FROM \`$tags_table\` ORDER BY metric_name, tags"
    echo "-- prometheusQuery"
    $TENANT -q "SELECT * FROM prometheusQuery($table, '{__name__=~\".+\"}', $T) ORDER BY 1"
    echo "-- prometheusQueryRange"
    $TENANT -q "SELECT * FROM prometheusQueryRange($table, 'up', $T - 60, $T, 30) ORDER BY 1"
    echo "-- promql dialect"
    $TENANT --dialect promql --promql_table "$table" --promql_evaluation_time "$T" -q 'count(up)'
    echo "-- samples table is not filtered"
    $TENANT -q "SELECT count() FROM timeSeriesSamples($table)"

    echo "--- $table: the default user reads everything ---"
    $CLIENT -q "SELECT metric_name, tags FROM $table ORDER BY metric_name, tags"
    $CLIENT -q "SELECT count() FROM prometheusQuery($table, '{__name__=~\".+\"}', $T)"

    echo "--- $table: the policy of the TimeSeries table is recorded for the PromQL query ---"
    local query_id="05427_${table}_${CLICKHOUSE_DATABASE}_$RANDOM"
    $TENANT --query_id "$query_id" -q "SELECT count() FROM prometheusQuery($table, 'up', $T)"
    $CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLIENT -q "SELECT arrayMap(x -> replaceAll(x, currentDatabase(), '<db>'), used_row_policies) FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"

    echo "--- $table: a policy on the inner tags table combines with the inherited one ---"
    $CLIENT -q "CREATE ROW POLICY only_up ON \`$tags_table\` FOR SELECT USING metric_name = 'up' TO $user"
    $TENANT -q "SELECT metric_name, tags FROM timeSeriesTags($table) ORDER BY metric_name, tags"
    $TENANT -q "SELECT count() FROM prometheusQuery($table, '{__name__=~\".+\"}', $T)"
    $CLIENT -q "DROP ROW POLICY only_up ON \`$tags_table\`"

    echo "--- $table: a policy on a column the inner tags table lacks works for SELECT and fails closed for PromQL ---"
    $CLIENT -q "DROP ROW POLICY tenant_a ON $table"
    $CLIENT -q "CREATE ROW POLICY has_samples ON $table FOR SELECT USING length(samples) > 0 TO $user"
    $TENANT -q "SELECT count() FROM $table"
    $TENANT -q "SELECT count() FROM prometheusQuery($table, 'up', $T) -- { serverError UNKNOWN_IDENTIFIER }"
    $TENANT -q "SELECT count() FROM timeSeriesTags($table) -- { serverError UNKNOWN_IDENTIFIER }"
    $CLIENT -q "DROP ROW POLICY has_samples, everything ON $table"
}

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries"
$CLIENT -q "CREATE TABLE ts_promoted ENGINE = TimeSeries SETTINGS tags_to_columns = {'tenant': 'tenant'}"
fill ts
fill ts_promoted

check_table ts
check_table ts_promoted

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ts, ts_promoted"
