#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# An `UPDATE` of the rows written by a previous `UPDATE`: `a` -> `b`, then `b` -> `c` leaves `c`. In format-version 3 the
# second `UPDATE` deletes rows of the data file the first one wrote, and the rows keep their `_row_id` through both.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_chained_updates"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

SETTINGS="--allow_insert_into_iceberg=1 --mutations_sync=1"

for format_version in 2 3; do
    echo "-- format-version ${format_version}"
    table="t${format_version}"
    ${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
        CREATE TABLE ${table} (k Int32, v String) ENGINE = IcebergLocal('${LAKE}/${table}')
            SETTINGS iceberg_format_version = ${format_version};
        INSERT INTO ${table} SELECT number, if(number < 10, 'a', 'x') FROM numbers(15);"
    row_ids_before=$(${CLICKHOUSE_CLIENT} --query "SELECT groupArray((k, _row_id)) FROM (SELECT k, _row_id FROM icebergLocal('${LAKE}/${table}') ORDER BY k)")

    ${CLICKHOUSE_CLIENT} ${SETTINGS} --query "ALTER TABLE ${table} UPDATE v = 'b' WHERE v = 'a';"
    ${CLICKHOUSE_CLIENT} ${SETTINGS} --query "ALTER TABLE ${table} UPDATE v = 'c' WHERE v = 'b';"

    ${CLICKHOUSE_CLIENT} --query "SELECT v, groupArray(k) FROM (SELECT k, v FROM icebergLocal('${LAKE}/${table}') ORDER BY k) GROUP BY v ORDER BY v"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM icebergLocal('${LAKE}/${table}')"

    row_ids_after=$(${CLICKHOUSE_CLIENT} --query "SELECT groupArray((k, _row_id)) FROM (SELECT k, _row_id FROM icebergLocal('${LAKE}/${table}') ORDER BY k)")
    if [ "${format_version}" = 3 ]; then
        [ "${row_ids_before}" = "${row_ids_after}" ] && echo "row ids kept" || echo "row ids changed: ${row_ids_before} -> ${row_ids_after}"
    fi
done

rm -rf "${LAKE}"
