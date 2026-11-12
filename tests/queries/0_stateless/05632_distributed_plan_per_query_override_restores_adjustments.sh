#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# A per-query override of distributed_plan_workers_num must restore the settings the session's own derived plan force-adjusted, for that query only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=${CLICKHOUSE_DATABASE}&session_id=05632_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET compile_expressions = 1" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET distributed_plan_workers_num = 3" > /dev/null

${CLICKHOUSE_CURL} -sS "${URL}&distributed_plan_workers_num=0" --data-binary "SELECT getSetting('compile_expressions')"
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SELECT getSetting('compile_expressions')"
