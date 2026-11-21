#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: that configuration replaces the user directories, so there is no `memory` storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A rename of a quota or a row policy with `IN` must not take a name used in another user directory.

source_quota="quota_05813_source_${CLICKHOUSE_DATABASE}"
target_quota="quota_05813_target_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP QUOTA IF EXISTS ${source_quota}, ${target_quota}"
${CLICKHOUSE_CLIENT} --query "CREATE QUOTA ${source_quota} IN local_directory"
${CLICKHOUSE_CLIENT} --query "CREATE QUOTA ${target_quota} IN memory"
${CLICKHOUSE_CLIENT} --query "ALTER QUOTA ${source_quota} IN local_directory RENAME TO ${target_quota}" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "ALTER QUOTA IF EXISTS ${source_quota} IN local_directory RENAME TO ${target_quota}" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "SELECT name = '${target_quota}', storage FROM system.quotas WHERE name IN ('${source_quota}', '${target_quota}') ORDER BY storage"
${CLICKHOUSE_CLIENT} --query "DROP QUOTA ${source_quota}, ${target_quota}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05813 (x UInt8) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS policy_05813_source, policy_05813_target ON ${CLICKHOUSE_DATABASE}.t_05813"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY policy_05813_source ON ${CLICKHOUSE_DATABASE}.t_05813 IN local_directory"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY policy_05813_target ON ${CLICKHOUSE_DATABASE}.t_05813 IN memory"
${CLICKHOUSE_CLIENT} --query "ALTER ROW POLICY policy_05813_source ON ${CLICKHOUSE_DATABASE}.t_05813 IN local_directory RENAME TO policy_05813_target" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "ALTER ROW POLICY IF EXISTS policy_05813_source ON ${CLICKHOUSE_DATABASE}.t_05813 IN local_directory RENAME TO policy_05813_target" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "SELECT short_name, storage FROM system.row_policies WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_05813' ORDER BY storage"

# A rename within the requested directory to a free name still works.
${CLICKHOUSE_CLIENT} --query "ALTER ROW POLICY policy_05813_source ON ${CLICKHOUSE_DATABASE}.t_05813 IN local_directory RENAME TO policy_05813_renamed"
${CLICKHOUSE_CLIENT} --query "SELECT short_name, storage FROM system.row_policies WHERE database = '${CLICKHOUSE_DATABASE}' AND table = 't_05813' ORDER BY storage"

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY policy_05813_renamed, policy_05813_target ON ${CLICKHOUSE_DATABASE}.t_05813"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05813"
