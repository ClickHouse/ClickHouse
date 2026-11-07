#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=helpers/query_access_info.sh
. "$CUR_DIR"/helpers/query_access_info.sh

cleanup()
{
    qai_cleanup_users mv_caller root_mv_caller invoker
}
trap cleanup EXIT

run_sql_input "$CUR_DIR"/05395_nested_mv_access_info.sql.in
