#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Time travel to an earlier snapshot reads a table whose metadata stores full file paths
# (write_full_path_in_iceberg_metadata), and the current snapshot still sees every row.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

INSERT_SETTINGS="SETTINGS allow_insert_into_iceberg = 1, write_full_path_in_iceberg_metadata = 1"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t (x String, y Int64) ENGINE = IcebergLocal('${ROOT}/t/')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t ${INSERT_SETTINGS} VALUES ('123', 1)"

first_snapshot=$(${CLICKHOUSE_CLIENT} --query "
    SELECT snapshot_id FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = 't' AND is_current_ancestor")

${CLICKHOUSE_CLIENT} --query "INSERT INTO t ${INSERT_SETTINGS} VALUES ('123', 1)"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t SETTINGS iceberg_snapshot_id = ${first_snapshot}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
