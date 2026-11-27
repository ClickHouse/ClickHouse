#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# An `UPDATE` of a format-version 3 Iceberg table deletes exactly the rows it rewrites, even if its predicate selects
# different rows each time it is evaluated: every key stays exactly once, with its own `_row_id`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_v3_update_nondeterministic"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

SETTINGS="--allow_insert_into_iceberg=1 --mutations_sync=1"

${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    CREATE TABLE t (k Int32, v String) ENGINE = IcebergLocal('${LAKE}/t') SETTINGS iceberg_format_version = 3;
    INSERT INTO t SELECT number, 'original' FROM numbers(1000);
    ALTER TABLE t UPDATE v = 'updated' WHERE rand() % 2 = 0;
    ALTER TABLE t UPDATE v = 'updated twice' WHERE rand() % 2 = 0;"

${CLICKHOUSE_CLIENT} --query "
    SELECT count(), uniqExact(k), uniqExact(_row_id), min(k), max(k), countIf(v != 'original') > 0
    FROM icebergLocal('${LAKE}/t')"

rm -rf "${LAKE}"
