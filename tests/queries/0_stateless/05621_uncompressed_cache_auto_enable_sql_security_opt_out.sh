#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage
# Tag no-random-settings: the test asserts uncompressed cache profile events, which a randomized
# `use_uncompressed_cache` would distort.
# Tag no-object-storage: automatic mode never applies to parts on object storage.

# An explicit `use_uncompressed_cache = 0` of the caller must win over `enable_automatic_use_uncompressed_cache = 1`
# also when the query context is rebuilt for another user: the body of a `SQL SECURITY DEFINER` view and
# `EXECUTE AS`. The rebuild clamps the caller's settings to the other user's constraints, which drops a change
# whose value equals the current one, so the default-valued opt-out was lost there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DEFINER_USER="definer_${CLICKHOUSE_DATABASE}"
CALLER_USER="caller_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${DEFINER_USER}, ${CALLER_USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${DEFINER_USER}, ${CALLER_USER}"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_sql_security (id UInt64, payload String)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
    INSERT INTO t_sql_security SELECT number, repeat('x', 128) FROM numbers(32768);
    CREATE VIEW v_sql_security DEFINER = ${DEFINER_USER} SQL SECURITY DEFINER AS SELECT payload FROM t_sql_security WHERE id >= 0;
"

$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_sql_security TO ${DEFINER_USER}"
$CLICKHOUSE_CLIENT --query "GRANT IMPERSONATE ON ${DEFINER_USER} TO ${CALLER_USER}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_sql_security TO ${CALLER_USER}"

COMMON_SETTINGS="SET log_queries = 1; SET max_threads = 1; SET use_query_condition_cache = 0; SET enable_automatic_use_uncompressed_cache = 1;"

# `SQL SECURITY DEFINER` view: the opt-out in the outer `SETTINGS` clause.
$CLICKHOUSE_CLIENT --query "${COMMON_SETTINGS}
    SELECT sum(length(payload)) = 32768 * 128 FROM v_sql_security SETTINGS log_comment = '05621_view_control';
    SELECT sum(length(payload)) = 32768 * 128 FROM v_sql_security SETTINGS log_comment = '05621_view_opt_out', use_uncompressed_cache = 0;
"

# `EXECUTE AS` with a wrapped query: the opt-out in the session.
$CLICKHOUSE_CLIENT --user "${CALLER_USER}" --query "${COMMON_SETTINGS}
    SET log_comment = '05621_execute_as_control';
    EXECUTE AS ${DEFINER_USER} SELECT sum(length(payload)) = 32768 * 128 FROM t_sql_security;
"
$CLICKHOUSE_CLIENT --user "${CALLER_USER}" --query "${COMMON_SETTINGS}
    SET log_comment = '05621_execute_as_opt_out';
    SET use_uncompressed_cache = 0;
    EXECUTE AS ${DEFINER_USER} SELECT sum(length(payload)) = 32768 * 128 FROM t_sql_security;
"

# `EXECUTE AS` switching the session: the opt-out set before the switch.
$CLICKHOUSE_CLIENT --user "${CALLER_USER}" --query "${COMMON_SETTINGS}
    SET log_comment = '05621_session_control';
    EXECUTE AS ${DEFINER_USER};
    SELECT sum(length(payload)) = 32768 * 128 FROM t_sql_security;
"
$CLICKHOUSE_CLIENT --user "${CALLER_USER}" --query "${COMMON_SETTINGS}
    SET log_comment = '05621_session_opt_out';
    SET use_uncompressed_cache = 0;
    EXECUTE AS ${DEFINER_USER};
    SELECT sum(length(payload)) = 32768 * 128 FROM t_sql_security;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT log_comment, max(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']) > 0
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - INTERVAL 10 MINUTE
      AND type = 'QueryFinish'
      AND log_comment LIKE '05621\\_%'
      AND current_database = currentDatabase()
    GROUP BY log_comment
    ORDER BY log_comment
"

$CLICKHOUSE_CLIENT --query "DROP VIEW v_sql_security"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_sql_security"
$CLICKHOUSE_CLIENT --query "DROP USER ${DEFINER_USER}, ${CALLER_USER}"
