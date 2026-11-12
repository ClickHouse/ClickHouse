#!/usr/bin/env bash
# Tags: no-old-analyzer, no-parallel
# no-old-analyzer: make_distributed_plan requires the analyzer.
# no-parallel: creates a server-global settings profile under a fixed name.

# Restoring a remembered pre-adjustment value must be judged under the current settings
# constraints, not just the ones active when the value was remembered: a profile switch
# mid-session can forbid a value the derived plan is about to restore. Before this was fixed, the
# restore below put automatic_parallel_replicas_mode back to 1, bypassing the profile's MAX 0 that
# arrived after the value was remembered.
#
# automatic_parallel_replicas_mode = 1 arrives through a profile, not a bare SET: clickhouse-client
# mirrors every bare SET and resends it with each later query it sends, but never a profile
# switch's own settings. A bare SET here would get resent on the SET below that installs the
# MAX 0 profile and be rejected outright, before the restore logic under test ever ran.
#
# A bare URL, not ${CLICKHOUSE_URL}: the harness bakes its own randomized settings into
# CLICKHOUSE_URL_PARAMS and resends them with every request built from it, which would pin
# automatic_parallel_replicas_mode via the URL on every statement here - including the final
# SELECT - independently of anything the fix under test does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=${CLICKHOUSE_DATABASE}&session_id=05631_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS profile_05631_value, profile_05631_const"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE profile_05631_value SETTINGS automatic_parallel_replicas_mode = 1"
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE profile_05631_const SETTINGS automatic_parallel_replicas_mode MAX 0"

${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET profile = 'profile_05631_value'" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET distributed_plan_workers_num = 3" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET profile = 'profile_05631_const'" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SET distributed_plan_workers_num = 0" > /dev/null
${CLICKHOUSE_CURL} -sS "${URL}" --data-binary "SELECT getSetting('automatic_parallel_replicas_mode')"
