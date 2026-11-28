#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The table functions returning the target tables of a TimeSeries table need `SHOW COLUMNS` on the TimeSeries table,
# as `DESCRIBE TABLE` of that table does.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    SET enable_time_series_table = 1;
    CREATE TABLE ts ENGINE = TimeSeries;
    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
    GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.ts TO ${username};
"

function as_user()
{
    echo "$1"
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${username}" --query "$1" 2>&1)
    if grep -q ACCESS_DENIED <<< "${output}"
    then
        echo ACCESS_DENIED
    else
        echo "${output}"
    fi
}

# The structure depends on the version of the TimeSeries table, so it is compared with the owner's instead of printed.
function same_as_owner()
{
    echo "$1"
    local expected actual
    expected=$(${CLICKHOUSE_CLIENT} --query "$1" 2>&1)
    actual=$(${CLICKHOUSE_CLIENT} --user "${username}" --query "$1" 2>&1)
    if [ -n "${expected}" ] && [ "${expected}" = "${actual}" ]
    then
        echo "same as for the owner"
    else
        echo "${actual}"
    fi
}

echo "--- SHOW TABLES"
as_user "DESCRIBE TABLE ts"
as_user "DESCRIBE TABLE timeSeriesSamples(currentDatabase(), 'ts')"
as_user "DESCRIBE TABLE timeSeriesData(ts)"
as_user "DESCRIBE TABLE timeSeriesTags('ts')"
as_user "DESCRIBE TABLE timeSeriesTimeRanges(ts)"
as_user "DESCRIBE TABLE timeSeriesMetricFamilies(ts)"
as_user "DESCRIBE TABLE timeSeriesMetrics(ts)"
as_user "DESCRIBE (SELECT * FROM timeSeriesSamples(ts))"
as_user "SELECT count() FROM timeSeriesSamples(ts)"

# A missing table must look the same as an existing one without access.
echo "--- missing table"
as_user "DESCRIBE TABLE timeSeriesSamples(no_such_table)"

echo "--- SHOW COLUMNS"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.ts TO ${username}"
same_as_owner "DESCRIBE TABLE timeSeriesSamples(ts)"
same_as_owner "DESCRIBE TABLE timeSeriesTags(ts)"

# `SELECT` on the TimeSeries table implies `SHOW COLUMNS` on it.
echo "--- SELECT"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.ts FROM ${username}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.ts TO ${username}"
same_as_owner "DESCRIBE TABLE timeSeriesSamples(ts)"
as_user "SELECT count() FROM timeSeriesSamples(ts)"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER ${username};
    DROP TABLE ts;
"
