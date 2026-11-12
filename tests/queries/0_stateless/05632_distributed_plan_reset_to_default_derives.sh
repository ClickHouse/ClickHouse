#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# `SET ... = DEFAULT` mutates settings through Context::resetSettingsToDefaultValue, a path
# separate from applySettingsChanges, so the implication from distributed_plan_workers_num must be
# re-established there too: an explicit make_distributed_plan = 0, reset back to DEFAULT while a
# non-zero distributed_plan_workers_num is still in effect, must derive the plan on again.
#
# Over the native protocol (clickhouse-client), this is hard to prove cleanly: the client mirrors
# every bare SET and resends it with each later query in the same session, so the final SELECT
# below would carry distributed_plan_workers_num = 3 as an explicit resent change of its own,
# alongside whatever resetSettingsToDefaultValue already derived - making it unclear which path
# produced the result. Plain HTTP requests do not mirror anything, so each one here carries only
# the one statement it sends; session_id keeps them in the same session.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SESSION_URL="${CLICKHOUSE_URL}&session_id=05632_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CURL} -sS "${SESSION_URL}" --data-binary "SET distributed_plan_workers_num = 3" > /dev/null
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" --data-binary "SET make_distributed_plan = 0" > /dev/null
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" --data-binary "SET make_distributed_plan = DEFAULT" > /dev/null
${CLICKHOUSE_CURL} -sS "${SESSION_URL}" --data-binary "SELECT getSetting('make_distributed_plan')"
