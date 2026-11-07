#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=helpers/query_access_info.sh
. "$CUR_DIR"/helpers/query_access_info.sh

cleanup()
{
    qai_cleanup_users mv_reader
}
trap cleanup EXIT

reader_user=$(qai_user_identifier "qai_${CLICKHOUSE_TEST_UNIQUE_NAME}_mv_reader")

run_sql_input "$CUR_DIR"/05395_materialized_view_read.before.sql.in
expect_access_denied "EXECUTE AS ${reader_user} SELECT count() FROM t_05402_legacy_target"
run_sql_input "$CUR_DIR"/05395_materialized_view_read.after.sql.in
