#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# `ALTER TABLE ... ADD COLUMN` and `RENAME COLUMN` must not give a column the name of a partition field derived from
# another column: the Java Iceberg implementation cannot load such a table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/t"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
}
cleanup
trap cleanup EXIT

alter()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} $1" 2>&1)
    if [[ -z "${output}" ]]; then
        echo "$1: OK"
    elif grep -q 'BAD_ARGUMENTS' <<< "${output}" && grep -q 'has the name of a field of partition spec 0' <<< "${output}"; then
        echo "$1: partition field name conflict"
    else
        echo "$1: unexpected output: ${output}"
    fi
}

metadata_files()
{
    find "${TABLE_PATH}/metadata" -name '*.metadata.json' | wc -l
}

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE ${TABLE} (ts DateTime64(6, 'UTC'), id Int64, i Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (icebergYear(ts), id);
    INSERT INTO ${TABLE} VALUES (toDateTime64('2024-01-01 05:00:00', 6, 'UTC'), 1, 10);"

files_before=$(metadata_files)
alter "ADD COLUMN ts_year Nullable(Int64)"
alter "RENAME COLUMN i TO ts_year"
echo "metadata files unchanged: $(( $(metadata_files) == files_before ))"

alter "ADD COLUMN other Nullable(Int64)"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES (toDateTime64('2023-01-01 05:00:00', 6, 'UTC'), 2, 20, NULL)"
${CLICKHOUSE_CLIENT} --query "SELECT i, id, other, icebergYear(ts) FROM ${TABLE} ORDER BY i"
