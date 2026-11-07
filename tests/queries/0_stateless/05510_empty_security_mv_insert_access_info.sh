#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=helpers/query_access_info.sh
. "$CUR_DIR"/helpers/query_access_info.sh

cleanup()
{
    qai_cleanup_users empty_security_caller
}
trap cleanup EXIT

caller_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_empty_security_caller")

run_sql_input "$CUR_DIR"/05395_empty_security_mv_insert.before.sql.in
expect_access_denied "EXECUTE AS ${caller_user} INSERT INTO t_05401_source_sink SETTINGS log_comment = '05401_direct_source_sink_insert' SELECT toUInt64(1)"
expect_access_denied "EXECUTE AS ${caller_user} INSERT INTO t_05401_root_sink SETTINGS log_comment = '05401_direct_root_sink_insert' SELECT toUInt64(2)"
run_sql_input "$CUR_DIR"/05395_empty_security_mv_insert.after.sql.in
