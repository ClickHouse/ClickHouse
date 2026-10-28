#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: rewrite rules are global server state
# no-fasttest: the result template uses the `s3` table function

# A query run in the background (`run_query_in_background`) is executed by a worker that re-parses
# the query as rewritten at dispatch. If that parse fails (here the rewritten query exceeds
# `max_parser_depth`, which the submitted query does not), `system.query_log` must still describe
# the query as submitted, with the applied rule, and must not expose the rewritten text with the
# credentials of the result template.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./04666_run_query_in_background.lib
. "$CUR_DIR"/04666_run_query_in_background.lib

rule="rule_05458_${CLICKHOUSE_DATABASE}"
nested="number"
for _ in {1..64}; do nested="toString($nested)"; done

$CLICKHOUSE_CLIENT -q "
    DROP RULE IF EXISTS ${rule};
    CREATE RULE ${rule} AS (SELECT 12345)
        REWRITE TO (SELECT ${nested} FROM s3('http://localhost:11111/test/05458.csv', 'key_05458', 'secret_05458', 'CSV', 'number UInt64'));
"

query_id="background_parse_failure_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&query_rules=${rule}&run_query_in_background=1&max_parser_depth=128&query_id=${query_id}" -d "SELECT 12345" \
    | grep -o -m1 "TOO_DEEP_AST"

wait_for_query_log "count() > 0 FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'ExceptionBeforeStart'"
$CLICKHOUSE_CLIENT -q "
    SELECT query, applied_rules = ['${rule}'], normalized_query_hash = normalizedQueryHash('SELECT 12345'), formatted_query NOT LIKE '%secret_05458%'
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'ExceptionBeforeStart';
    SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND query LIKE '%secret_05458%';
"

$CLICKHOUSE_CLIENT -q "DROP RULE ${rule}"
