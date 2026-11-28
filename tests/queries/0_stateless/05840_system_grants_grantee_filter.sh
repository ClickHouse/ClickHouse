#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each run gets its own access storage, including under the parallel flaky check.
${CLICKHOUSE_LOCAL} --multiquery <<'SQL'
CREATE USER test_grants_filter_user;
CREATE ROLE test_grants_filter_role;
GRANT SELECT ON test_grants_filter_db.readable TO test_grants_filter_user;
GRANT INSERT ON test_grants_filter_db.writable TO test_grants_filter_role;

SELECT user_name, role_name, access_type FROM system.grants
WHERE user_name = 'test_grants_filter_user' AND database = 'test_grants_filter_db';

SELECT user_name, role_name, access_type FROM system.grants
WHERE (user_name IN ('test_grants_filter_user', 'test_grants_filter_missing', NULL)
       OR 'test_grants_filter_role' = role_name)
  AND database = 'test_grants_filter_db'
ORDER BY access_type::String DESC;

SELECT count() FROM system.grants
WHERE user_name = 'test_grants_filter_user' AND role_name = 'test_grants_filter_role';

SELECT count() FROM system.grants
WHERE database = 'test_grants_filter_db'
  AND (user_name = 'test_grants_filter_user' OR table = 'writable');

SELECT count() FROM system.grants WHERE role_name = 'test_grants_filter_missing';

SQL
