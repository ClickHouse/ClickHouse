#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: that configuration replaces the user directories, so there is no `memory` storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user name must not be used in more than one user directory.
# `memory` is the last user directory, so `CREATE USER` without `IN` would pick an earlier one.

user="user_05770_${CLICKHOUSE_DATABASE}"
source_user="user_05770_source_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}, ${source_user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'p1' IN memory"

${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IN local_directory" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "CREATE USER IF NOT EXISTS ${user}"

# A replacement is refused if another directory holds the name, but allowed within the same directory.
${CLICKHOUSE_CLIENT} --query "CREATE USER OR REPLACE ${user} IDENTIFIED WITH plaintext_password BY 'p2'" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "CREATE USER OR REPLACE ${user} IDENTIFIED WITH plaintext_password BY 'p2' IN local_directory" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "SELECT storage FROM system.users WHERE name = '${user}'"
${CLICKHOUSE_CLIENT} --user "${user}" --password p1 --query "SELECT currentUser() = '${user}'"
${CLICKHOUSE_CLIENT} --query "CREATE USER OR REPLACE ${user} IDENTIFIED WITH plaintext_password BY 'p2' IN memory"
${CLICKHOUSE_CLIENT} --user "${user}" --password p2 --query "SELECT currentUser() = '${user}'"

# A rename to a name held by another directory is refused, and both users remain usable.
${CLICKHOUSE_CLIENT} --query "CREATE USER ${source_user} IDENTIFIED WITH plaintext_password BY 'p3' IN local_directory"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${source_user} RENAME TO ${user}" 2>&1 | grep -o -m1 "ACCESS_ENTITY_ALREADY_EXISTS"
${CLICKHOUSE_CLIENT} --query "SELECT name = '${user}', storage FROM system.users WHERE name IN ('${user}', '${source_user}') ORDER BY storage"
${CLICKHOUSE_CLIENT} --user "${user}" --password p2 --query "SELECT currentUser() = '${user}'"
${CLICKHOUSE_CLIENT} --user "${source_user}" --password p3 --query "SELECT currentUser() = '${source_user}'"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}, ${source_user}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.users WHERE name IN ('${user}', '${source_user}')"
