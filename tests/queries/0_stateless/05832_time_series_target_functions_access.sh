#!/usr/bin/env bash

# The functions timeSeriesSamples, timeSeriesTags, timeSeriesTimeRanges and timeSeriesMetricFamilies return a target table
# of a TimeSeries table, so they need the grant on both tables: SHOW COLUMNS for the structure, SELECT to read, INSERT to write.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1 -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user};
    CREATE TABLE ${db}.ts ENGINE = TimeSeries;
    INSERT INTO ${db}.ts (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 1)]);
"

function run()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" -q "$1" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'OK'
}

function all()
{
    for f in timeSeriesSamples timeSeriesData timeSeriesTags timeSeriesTimeRanges timeSeriesMetricFamilies timeSeriesMetrics; do
        echo "${f}: describe $(run "DESCRIBE TABLE ${f}(${db}.ts)"), select $(run "SELECT * FROM ${f}(${db}.ts) FORMAT Null")"
    done
    echo "insert: $(run "INSERT INTO FUNCTION timeSeriesMetricFamilies(${db}.ts) (metric_family, type, help, unit) VALUES ('injected', 'gauge', '', '')")"
}

echo '--- no grants'
all

echo '--- SELECT on the TimeSeries table only'
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.ts TO ${user}"
all

echo '--- SELECT on the target tables only'
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON ${db}.ts FROM ${user}"
for target in $(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE '.inner%'"); do
    ${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.\`${target}\` TO ${user}"
done
all

echo '--- SELECT on the database'
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${db}.* TO ${user}"
all

echo '--- SELECT and INSERT on the database'
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON ${db}.* TO ${user}"
all

echo '--- metric families'
${CLICKHOUSE_CLIENT} -q "SELECT metric_family FROM timeSeriesMetricFamilies(${db}.ts)"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
