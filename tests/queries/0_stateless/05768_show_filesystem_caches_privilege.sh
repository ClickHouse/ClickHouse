#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="u_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS \`${USER_NAME}\`"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "
    CREATE USER \`${USER_NAME}\` NOT IDENTIFIED;
    GRANT SELECT ON system.filesystem_cache_settings TO \`${USER_NAME}\`"

echo "--- without SHOW FILESYSTEM CACHES ---"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "CHECK GRANT SHOW FILESYSTEM CACHES ON *.*"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "SHOW FILESYSTEM CACHES" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "DESCRIBE FILESYSTEM CACHE 'nonexistent'" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "SELECT count() FROM system.filesystem_cache_settings" 2>&1 | grep -o -m1 'ACCESS_DENIED'

${CLICKHOUSE_CLIENT} -q "GRANT SHOW FILESYSTEM CACHES ON *.* TO \`${USER_NAME}\`"

echo "--- with SHOW FILESYSTEM CACHES ---"
${CLICKHOUSE_CLIENT} -q "SELECT toString(access_type) AS a, database, table FROM system.grants WHERE user_name = '${USER_NAME}' ORDER BY a"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "CHECK GRANT SHOW FILESYSTEM CACHES ON *.*"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "SHOW FILESYSTEM CACHES FORMAT Null" && echo "show ok"
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "DESCRIBE FILESYSTEM CACHE 'nonexistent'" 2>&1 | grep -o -m1 'There is no cache by name'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "SELECT count() FROM system.filesystem_cache_settings FORMAT Null" && echo "select ok"

echo "--- it is a global privilege ---"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW FILESYSTEM CACHES ON system.* TO \`${USER_NAME}\`" 2>&1 | grep -o -m1 'INVALID_GRANT'

echo "--- no privilege group other than NONE is empty ---"
${CLICKHOUSE_CLIENT} -q "
    SELECT privilege FROM system.privileges
    WHERE level IS NULL AND privilege != 'NONE'
        AND toString(privilege) NOT IN (SELECT toString(parent_group) FROM system.privileges WHERE parent_group IS NOT NULL)"
