#!/usr/bin/env bash
# Tags: no-old-analyzer, no-parallel
# no-old-analyzer: make_distributed_plan requires the analyzer.
# no-parallel: creates a server-global settings profile and user under a fixed name.

# make_distributed_plan is derived fresh for every query from whatever currently wants it - a
# profile's own distributed_plan_workers_num included - never once for the session: a later query
# in the same native-protocol session that explicitly turns the workers num back to zero must see
# every setting the plan would have adjusted (compile_expressions here) at its plain, unadjusted
# default, not at a value carried over from an earlier query that did derive the plan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE="p_05633_${CLICKHOUSE_DATABASE}"
USER="u_05633_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS distributed_plan_workers_num = 3"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} SETTINGS PROFILE ${PROFILE}"

${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" --user "${USER}" --multiquery --query "
SET distributed_plan_workers_num = 0;
SELECT getSetting('compile_expressions');
"
