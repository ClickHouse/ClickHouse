#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: that configuration replaces the user directories, so there is no `memory` storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user name must not be used in more than one user directory.
# `memory` is the last user directory, so `CREATE USER` without `IN` would pick an earlier one.

user="user_05770_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IN memory"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IN local_directory" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "CREATE USER IF NOT EXISTS ${user}"

${CLICKHOUSE_CLIENT} --query "SELECT storage FROM system.users WHERE name = '${user}'"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.users WHERE name = '${user}'"
