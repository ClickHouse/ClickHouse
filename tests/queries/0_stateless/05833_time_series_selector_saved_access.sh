#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# A table created AS timeSeriesSelector keeps the parsed function, so every read checks the SELECT grant on the source again,
# even with the grants on the saved table and on the inner tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    CREATE TABLE ${db}.ts ENGINE = TimeSeries;
    INSERT INTO ${db}.ts (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 7)]);
    CREATE TABLE ${db}.saved AS timeSeriesSelector(${db}.ts, 'm', 0, 140, 'timestamp_and_value');
    GRANT SELECT ON ${db}.saved TO ${user};
"
for target in $(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE '.inner%'"); do
    ${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.\`${target}\` TO ${user}"
done

function read_saved()
{
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT value FROM ${db}.saved" 2>&1)
    echo "${out}" | grep -o -m1 'ACCESS_DENIED' || echo "${out}"
}

echo '--- read by the creator'
${CLICKHOUSE_CLIENT} -q "SELECT value FROM ${db}.saved"

echo '--- no grant on the source'
read_saved

echo '--- SELECT on the source'
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ts TO ${user}"
read_saved

echo '--- SELECT on the source revoked'
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON ${db}.ts FROM ${user}"
read_saved

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
