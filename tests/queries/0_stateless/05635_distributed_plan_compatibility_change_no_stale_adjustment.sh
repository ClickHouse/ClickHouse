#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# make_distributed_plan is derived fresh for every query, so nothing it adjusts is ever carried
# over from an earlier query. A `compatibility` change between a query that derives the plan and
# one that does not must not leave the later query's adjusted settings (compile_expressions here)
# at a value computed for an earlier query - it must see the session's own current value, exactly
# as if the plan had never been derived in between.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=${CLICKHOUSE_DATABASE}&session_id=05635_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET distributed_plan_workers_num = 3" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET compatibility = '25.4'" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET distributed_plan_workers_num = 0" > /dev/null

${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SELECT getSetting('compile_expressions')"
