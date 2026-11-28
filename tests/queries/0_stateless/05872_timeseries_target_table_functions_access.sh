#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}_ts"
table="ts_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --multiquery "
    DROP USER IF EXISTS $user;
    DROP TABLE IF EXISTS $table;
    CREATE TABLE $table ENGINE = TimeSeries;
    CREATE USER $user;
    GRANT SHOW TABLES, SHOW COLUMNS ON $CLICKHOUSE_DATABASE.$table TO $user;
"

for f in timeSeriesSamples timeSeriesTags timeSeriesTimeRanges timeSeriesMetricFamilies; do
    $CLICKHOUSE_CLIENT --user $user --query "SELECT count() FROM $f('$CLICKHOUSE_DATABASE', '$table')" 2>&1 | grep -o "ACCESS_DENIED" | head -1
done

$CLICKHOUSE_CLIENT --multiquery "GRANT SELECT ON $CLICKHOUSE_DATABASE.$table TO $user"
$CLICKHOUSE_CLIENT --user $user --query "SELECT count() FROM timeSeriesSamples('$CLICKHOUSE_DATABASE', '$table')"

$CLICKHOUSE_CLIENT --multiquery "DROP USER $user; DROP TABLE $table SYNC;"
