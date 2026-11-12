#!/usr/bin/env bash
# Tags: no-old-analyzer, no-parallel
# no-old-analyzer: make_distributed_plan requires the analyzer.
# no-parallel: creates a server-global settings profile under a fixed name.

# Restoring a remembered pre-adjustment value must not be vetoed just because the setting is
# pinned CONST to that same value: a profile can lock a setting to a fixed value for the whole
# session, and the derived plan overriding it for a while must not turn that lock into a reset to
# the setting's unrelated default once the plan turns off again.
#
# A bare URL, not ${CLICKHOUSE_URL}: the harness bakes its own randomized settings into
# CLICKHOUSE_URL_PARAMS and resends them with every request built from it, which would pin
# automatic_parallel_replicas_mode via the URL on every statement here, independently of anything
# the fix under test does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=${CLICKHOUSE_DATABASE}&session_id=05633_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS profile_05633_const"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE profile_05633_const SETTINGS automatic_parallel_replicas_mode = 1 CONST"

${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET profile = 'profile_05633_const'" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET distributed_plan_workers_num = 3" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET distributed_plan_workers_num = 0" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SELECT getSetting('automatic_parallel_replicas_mode')"
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET automatic_parallel_replicas_mode = 1" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SELECT getSetting('automatic_parallel_replicas_mode')"
