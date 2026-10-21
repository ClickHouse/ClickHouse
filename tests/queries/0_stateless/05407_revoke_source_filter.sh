#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local enables `access_control_improvements.enable_read_write_grants`, so `system.grants` shows the filters.
${CLICKHOUSE_LOCAL} --multiquery <<'EOF'
CREATE USER u;

SELECT '-- revoke one of two filters';
GRANT READ ON URL('https://foo/.*') TO u;
GRANT READ ON URL('https://bar/.*') TO u;
REVOKE READ ON URL('https://foo/.*') FROM u;
SELECT access_type, access_object, is_partial_revoke, grant_option FROM system.grants WHERE user_name = 'u' ORDER BY ALL;

SELECT '-- revoke a filter which is not granted';
REVOKE READ ON URL('https://foo/.*') FROM u;
SELECT access_type, access_object, is_partial_revoke, grant_option FROM system.grants WHERE user_name = 'u' ORDER BY ALL;

SELECT '-- revoke the only filter';
REVOKE READ ON URL('https://bar/.*') FROM u;
SELECT count() FROM system.grants WHERE user_name = 'u';

SELECT '-- revoke without a filter';
GRANT READ ON URL('https://foo/.*') TO u;
GRANT READ ON URL('https://bar/.*') TO u;
REVOKE READ ON URL FROM u;
SELECT count() FROM system.grants WHERE user_name = 'u';

SELECT '-- revoke a filter under the unfiltered grant';
GRANT READ ON URL TO u;
REVOKE READ ON URL('https://foo/.*') FROM u; -- { serverError BAD_ARGUMENTS }
REVOKE GRANT OPTION FOR READ ON URL('https://foo/.*') FROM u;
SELECT access_type, access_object, is_partial_revoke, grant_option FROM system.grants WHERE user_name = 'u' ORDER BY ALL;
REVOKE READ ON URL FROM u;
GRANT READ ON * TO u WITH GRANT OPTION;
REVOKE GRANT OPTION FOR READ ON URL('https://foo/.*') FROM u; -- { serverError BAD_ARGUMENTS }
SELECT access_type, access_object, is_partial_revoke, grant_option FROM system.grants WHERE user_name = 'u' ORDER BY ALL;
REVOKE READ ON * FROM u;

SELECT '-- revoke the grant option of one filter';
GRANT READ ON URL('https://foo/.*') TO u WITH GRANT OPTION;
GRANT READ ON URL('https://bar/.*') TO u WITH GRANT OPTION;
REVOKE GRANT OPTION FOR READ ON URL('https://foo/.*') FROM u;
SELECT access_type, access_object, is_partial_revoke, grant_option FROM system.grants WHERE user_name = 'u' ORDER BY ALL;
EOF
