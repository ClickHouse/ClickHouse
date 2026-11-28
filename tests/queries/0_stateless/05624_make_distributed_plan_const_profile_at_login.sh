#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# The derivation of make_distributed_plan from distributed_plan_workers_num is checked against the
# settings constraints in force for the query (SettingsConstraints::allowsValue), without the
# readonly-mode restriction the ordinary constraint check otherwise enforces for an explicit change:
# deriving the plan is the server's own call, not something a readonly session asked for.
#
# A `const` constraint that pins the plan ON (not just one that forbids it) must not be refused, and
# one that forbids it must still veto a worker setting that wants the plan - exactly as it does for
# an explicit `SET make_distributed_plan = 1` (see 05623/05625).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

READONLY_USER="u_readonly_${CLICKHOUSE_TEST_UNIQUE_NAME}"
READONLY_PROFILE="p_readonly_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PINNED_ON_USER="u_pinned_on_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PINNED_ON_PROFILE="p_pinned_on_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PINNED_OFF_USER="u_pinned_off_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PINNED_OFF_PROFILE="p_pinned_off_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${READONLY_USER}, ${PINNED_ON_USER}, ${PINNED_OFF_USER}"
    ${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${READONLY_PROFILE}, ${PINNED_ON_PROFILE}, ${PINNED_OFF_PROFILE}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${READONLY_PROFILE} SETTINGS readonly = 1, distributed_plan_workers_num = 3"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${READONLY_USER} SETTINGS PROFILE ${READONLY_PROFILE}"

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PINNED_ON_PROFILE} SETTINGS make_distributed_plan = 1 CONST"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${PINNED_ON_USER} SETTINGS PROFILE ${PINNED_ON_PROFILE}"

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PINNED_OFF_PROFILE} SETTINGS distributed_plan_workers_num = 3, make_distributed_plan CONST"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${PINNED_OFF_USER} SETTINGS PROFILE ${PINNED_OFF_PROFILE}"

check()
{
    local label=$1 user=$2 setting=$3 expected=$4
    local actual
    # The bare binary, not ${CLICKHOUSE_CLIENT}: the harness options it carries (send_logs_level,
    # log_comment, randomized settings) are rejected at query start by a readonly=1 session.
    actual=$(${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${user}" -q "SELECT getSetting('${setting}')")
    echo "${label}"
    if [ "${actual}" == "${expected}" ]; then echo "OK"; else echo "FAIL: got ${actual}, expected ${expected}"; fi
}

check "still derived in a readonly session" "${READONLY_USER}" make_distributed_plan "true"
check "pinned on by a const constraint runs queries fine" "${PINNED_ON_USER}" make_distributed_plan "true"

echo "vetoed by the const constraint"
PINNED_ERROR=$(${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${PINNED_OFF_USER}" -q "SELECT 1" 2>&1)
if [[ "${PINNED_ERROR}" == *SETTING_CONSTRAINT_VIOLATION* ]]; then echo "OK"; else echo "FAIL: ${PINNED_ERROR}"; fi
