#!/usr/bin/env bash

# A query run in the background and rewritten by a rule is handed to the worker as the text this
# server formatted from the result template. That text can be longer than the submitted query, so the
# worker must not reject it by the `max_query_size` of the request, as the foreground run does not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

# Rule names are global; make them unique per test database so the test is parallel-safe.
RULE="rule_bg_mqs_${CLICKHOUSE_DATABASE}"
# `max_query_size` also bounds the value of `query_rules`, so it must fit the rule name, while the
# formatted result template below is longer than that.
MAX_QUERY_SIZE=100

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    CREATE RULE ${RULE} AS (SELECT 12345)
        REWRITE TO (INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT 42 WHERE 'a string that makes the result longer than max_query_size' != '');
"

# Foreground.
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&max_query_size=${MAX_QUERY_SIZE}&query_rules=${RULE}&async_insert=0" --data-binary "SELECT 12345"

# Background.
query_id="background_rewrite_max_query_size_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&max_query_size=${MAX_QUERY_SIZE}&query_rules=${RULE}&run_query_in_background=1&async_insert=0&query_id=${query_id}" --data-binary "SELECT 12345"
# Wait for the outcome either way, so that a failure of the worker shows as a missing row, not a timeout.
wait_for_query_log "count() = 1 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type != 'QueryStart'"

$CLICKHOUSE_CLIENT -q "SELECT x FROM t"

# The result template alone does not fit into `max_query_size`.
$CLICKHOUSE_CLIENT --max_query_size ${MAX_QUERY_SIZE} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT 42 WHERE 'a string that makes the result longer than max_query_size' != ''" 2>&1 | grep -o -m1 "Max query size exceeded"

$CLICKHOUSE_CLIENT -q "
    DROP RULE ${RULE};
    DROP TABLE t;
"
