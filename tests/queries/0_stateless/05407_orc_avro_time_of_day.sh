#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires the ORC and Avro formats
#
# `Time` and `Time64` are written as a time of day: an ORC `long` of microseconds with the Iceberg
# attribute `iceberg.long-type` = `TIME`, an Avro `long` with logical type `time-micros`. They are
# read back as `Time64(6)`, and values outside of [00:00:00, 24:00:00) are rejected on write.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/05407_time_of_day"
mkdir -p "${DATA_DIR}"

COLUMNS="toTime('12:34:56') AS t, toTime64('12:34:56.789', 3) AS t3, toTime64('23:59:59.999999', 6) AS t6, CAST(NULL, 'Nullable(Time64(6))') AS n"

for format in ORC Avro; do
    echo "--- ${format} ---"
    file="${DATA_DIR}/time.${format}"
    ${CLICKHOUSE_LOCAL} --query "SELECT ${COLUMNS} UNION ALL SELECT ${COLUMNS} FORMAT ${format}" > "${file}"
    ${CLICKHOUSE_LOCAL} --query "
        SELECT toTypeName(t), toTypeName(t3), toTypeName(t6), toTypeName(n), t, t3, t6, n FROM file('${file}') LIMIT 1"
    ${CLICKHOUSE_LOCAL} --query "SELECT t, t3, t6 FROM file('${file}', ${format}, 't Time, t3 Time64(3), t6 Time64(6)') LIMIT 1"
    # Read as an integer, the column is the stored microseconds.
    ${CLICKHOUSE_LOCAL} --query "SELECT t, t6 FROM file('${file}', ${format}, 't Int64, t6 Int64') LIMIT 1"
    # A filter on the column, which the ORC reader pushes down.
    ${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${file}') WHERE t6 = '23:59:59.999999'"
    ${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${file}') WHERE t6 < '23:59:59'"

    ${CLICKHOUSE_LOCAL} --query "SELECT toTime('100:00:00') AS t FORMAT ${format}" 2>&1 >/dev/null | grep -o -m1 'Value 100:00:00 of type Time is out of range' | head -n1
    ${CLICKHOUSE_LOCAL} --query "SELECT toTime64('-00:00:01', 6) AS t FORMAT ${format}" 2>&1 >/dev/null | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE' | head -n1
    ${CLICKHOUSE_LOCAL} --query "SELECT toTime64('12:00:00', 9) AS t FORMAT ${format}" 2>&1 >/dev/null | grep -o -m1 'ILLEGAL_COLUMN' | head -n1
    # A value under NULL is not checked.
    ${CLICKHOUSE_LOCAL} --query "SELECT if(number = 0, NULL, toTime('01:00:00')) AS t FROM numbers(2) FORMAT ${format}" > "${file}"
    ${CLICKHOUSE_LOCAL} --query "SELECT t FROM file('${file}')"
done

echo "--- ORC long without the attribute ---"
${CLICKHOUSE_LOCAL} --query "SELECT 45296000000::Int64 AS t FORMAT ORC" > "${DATA_DIR}/long.orc"
${CLICKHOUSE_LOCAL} --query "SELECT toTypeName(t), t FROM file('${DATA_DIR}/long.orc')"

rm -rf "${DATA_DIR}"
