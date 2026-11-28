#!/usr/bin/env bash
# Tags: no-parallel, no-flaky-check, no-msan
# - no-parallel - can be quite intense
# - no-flaky-check - too slow under ASan
# Random settings limits: max_bytes_before_external_group_by=(0, 0)
# - the aggregation spill threshold is compared against whole-query memory, not the hash table,
#   so a small random value makes every one of these queries spill per block (600s timeout)

# Check that all queries from the filesystem cache dashboards in system.dashboards are correct.
# Split out of 03540_system_dashboards. All ~160 queries are generated from one template (see
# StorageSystemDashboardsFilesystemCache.cpp), so the parameter handling is the same in all of them:
# running every query in all three variants took up to 600s under TSan with S3 storage.
# - every query runs over a range without data, which still analyzes it (column names, types)
# - the queries of the overview dashboard and the first one of the per-host dashboard also read the last 10 minutes
# - the first query of each dashboard also reads everything since a far `from`

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# - replace default with test_shard_localhost (CI does not have default cluster)
# - replace log_ with log$ to avoid reading all tables matching $log_ pattern, since this also includes distributed tables from ci-logs
mapfile -t queries <<<"$($CLICKHOUSE_CURL -sSk "${CLICKHOUSE_URL}&default_format=TSVRaw" -d "
    SELECT
        row_number() OVER (PARTITION BY dashboard) AS n,
        dashboard = 'Filesystem cache overview' OR n = 1 AS read_recent,
        n = 1 AS read_far,
        encodeURLComponent(formatQuerySingleLine(replace(replace(query, '(default', '(test_shard_localhost'), '_log', '_log\$')))
    FROM system.dashboards
    WHERE dashboard LIKE 'Filesystem cache%'")"
$CLICKHOUSE_CURL -sSk "${CLICKHOUSE_URL}" -d "SYSTEM FLUSH LOGS /* all tables */"
for line in "${queries[@]}"; do
  IFS=$'\t' read -r _ read_recent read_far q <<<"$line"
  # 1 hour range without data
  echo "${CLICKHOUSE_URL}&param_rounding=60&param_seconds=600&param_from=2020-01-01%2013%3A11%3A11&param_to=2020-01-01%2014%3A11%3A11&serialize_query_plan=0&default_format=Null&query=$q"
  if [[ $read_recent == 1 ]]; then
    # Last seconds
    echo "${CLICKHOUSE_URL}&param_rounding=60&param_seconds=600&param_from=&param_to=&serialize_query_plan=0&default_format=Null&query=$q"
  fi
  if [[ $read_far == 1 ]]; then
    # Far from parameter
    echo "${CLICKHOUSE_URL}&param_rounding=60&param_seconds=600&param_from=2020-01-01%2013%3A11%3A11&param_to=&serialize_query_plan=0&default_format=Null&query=$q"
  fi
done | {
  # - -n1 - we need real parallellism
  xargs -n1 -P10 $CLICKHOUSE_CURL -sSk
}
