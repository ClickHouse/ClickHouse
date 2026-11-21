#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# A query result cache hit is served without the planner's checks of the columns the query reads.
# With `query_cache_use_only_when_data_was_not_changed`, a revoke of `SELECT` on one column changes
# neither the table nor the cache key, so the result cached before the revoke must not be served while
# the user still reads another column: the read has to run again and fail with `ACCESS_DENIED`.

# Users are server-wide, so make the name unique per run.
reader="reader_05828_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, allow_experimental_parallel_reading_from_replicas = 0"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05828 (public_col UInt64, secret_col UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_05828 VALUES (1, 10), (2, 20), (3, 30);
    CREATE USER ${reader} IDENTIFIED WITH no_password;
    GRANT SELECT(public_col, secret_col) ON ${CLICKHOUSE_DATABASE}.t_05828 TO ${reader};
"

query="SELECT sum(secret_col) FROM ${CLICKHOUSE_DATABASE}.t_05828 SETTINGS ${qc}"

# Two runs of the same read: the second one is served from the cache (positive control).
$CLICKHOUSE_CLIENT --user "${reader}" -q "${query}"
$CLICKHOUSE_CLIENT --user "${reader}" -q "${query}"

# The reader loses access to the column the query reads, but keeps access to another one.
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(secret_col) ON ${CLICKHOUSE_DATABASE}.t_05828 FROM ${reader}"
$CLICKHOUSE_CLIENT --user "${reader}" -q "${query}" 2>&1 | grep -o -m1 'ACCESS_DENIED'

# Cache hits per successful run: the first run stores, the second hits.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT 'hits', groupArray(hits)
FROM
(
    SELECT ProfileEvents['QueryCacheHits'] AS hits
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
      AND user = '${reader}'
      AND query LIKE 'SELECT sum(secret_col) FROM ${CLICKHOUSE_DATABASE}.t_05828%'
    ORDER BY event_time_microseconds
)"

$CLICKHOUSE_CLIENT -q "
    DROP USER ${reader};
    DROP TABLE t_05828;
"
