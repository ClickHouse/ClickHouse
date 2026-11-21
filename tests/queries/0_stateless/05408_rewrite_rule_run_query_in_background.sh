#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: rewrite rules are global server state

# A query run in the background (`run_query_in_background`) is executed by a worker that re-parses
# the query text. The worker must execute the query as rewritten by the rules at dispatch,
# not the original text.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

$CLICKHOUSE_CLIENT -q "
    DROP RULE IF EXISTS rule_background_rewrite;
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    CREATE RULE rule_background_rewrite AS (SELECT 12345)
        REWRITE TO (INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT 42 SETTINGS query_rules = '');
"

query_id="background_rewrite_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&query_rules=rule_background_rewrite&run_query_in_background=1&async_insert=0&query_id=${query_id}" -d "SELECT 12345"
wait_for_query_log "$(finished_in_query_log "$query_id")"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t"
# The worker executes the rewritten query, but `system.query_log` describes the query as submitted
# and lists the rule applied at dispatch.
$CLICKHOUSE_CLIENT -q "
    SELECT query, applied_rules, normalized_query_hash = normalizedQueryHash('SELECT 12345')
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT -q "
    DROP RULE rule_background_rewrite;
    DROP TABLE t;
"
