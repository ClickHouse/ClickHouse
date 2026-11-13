#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_delta"

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_delta_kernel_rs=1 --allow_delta_lake_writes=1 --allow_delta_lake_create_table=1 --delta_lake_reload_schema_for_consistency=1"

cleanup()
{
    $CLIENT --query "DROP TABLE IF EXISTS t_dl"
    rm -rf "$TABLE_PATH"
}
trap cleanup EXIT

rm -rf "$TABLE_PATH"

$CLIENT --query "CREATE TABLE t_dl (k Int32, s String DEFAULT 'x', CONSTRAINT k_positive CHECK k > 0) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet)"
$CLIENT --query "INSERT INTO t_dl (k) VALUES (1)"
$CLIENT --query "INSERT INTO t_dl (k) VALUES (-1)" 2>&1 | grep -o -m1 VIOLATED_CONSTRAINT
$CLIENT --query "SELECT * FROM t_dl ORDER BY k"
