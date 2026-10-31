#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: rewrite rules are global server state

# A query run in the background and rewritten by a rule is handed to the worker as ClickHouse SQL,
# so the worker must parse it as such even when the request was submitted in another dialect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

$CLICKHOUSE_CLIENT -q "
    DROP RULE IF EXISTS rule_05446;
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    CREATE RULE rule_05446 AS (SELECT 12345)
        REWRITE TO (INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT 42 SETTINGS query_rules = '');
"

query_json=$($CLICKHOUSE_CLIENT -q "SELECT parseQueryToJSON('SELECT 12345')")
query_id="background_rewrite_json_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1&query_rules=rule_05446&run_query_in_background=1&async_insert=0&query_id=${query_id}" --data-binary "$query_json"
wait_for_query_log "$(finished_in_query_log "$query_id")"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t"

$CLICKHOUSE_CLIENT -q "
    DROP RULE rule_05446;
    DROP TABLE t;
"
