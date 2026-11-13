#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Reading a `SQL SECURITY DEFINER` materialized view requires the definer's `SELECT` on the source table
# of the view, not only on its target table. With `query_cache_use_only_when_data_was_not_changed`, a
# revoke of that grant leaves the target unchanged, so the result cached before the revoke must not be
# served: the read has to run again and fail with `ACCESS_DENIED`.

# Users are server-wide, so make the name unique per run.
definer="definer_05629_${CLICKHOUSE_DATABASE}"

# Pin every query-cache setting so the settings randomizer cannot change the outcome.
qc="use_query_cache = 1, enable_reads_from_query_cache = 1, enable_writes_to_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, allow_experimental_parallel_reading_from_replicas = 0"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src_05629 TO ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.target_05629 TO ${definer};

    CREATE TABLE src_05629 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE target_05629 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO target_05629 VALUES (1), (2), (3);

    CREATE MATERIALIZED VIEW mv_05629 TO target_05629 (x UInt64)
        DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src_05629;
"

# Two runs of the same read: the second one is served from the cache (positive control).
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM mv_05629 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM mv_05629 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}"

# The definer loses access to the source table, but keeps access to the target table.
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.src_05629 FROM ${definer}"
$CLICKHOUSE_CLIENT -q "SELECT sum(x) FROM mv_05629 WHERE '${CLICKHOUSE_DATABASE}' != '' SETTINGS ${qc}" 2>&1 | grep -o -m1 'ACCESS_DENIED'

# Cache hits per successful run: the first run stores, the second hits.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT 'hits', groupArray(hits)
FROM
(
    SELECT ProfileEvents['QueryCacheHits'] AS hits
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND query LIKE 'SELECT sum(x) FROM mv_05629 WHERE%'
    ORDER BY event_time_microseconds
)"

# The view's modification hash is `NULL` while the definer cannot read the source table.
$CLICKHOUSE_CLIENT -q "SELECT modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 'mv_05629'"

# A user cannot be dropped while it is the definer of a view.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_05629;
    DROP TABLE target_05629;
    DROP TABLE src_05629;
    DROP USER ${definer};
"
