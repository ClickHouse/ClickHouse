#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# The Avro writer writes the keys of a `Map` as strings, while Iceberg requires a `time` map key in an
# Avro data file to be a `time-micros` `long` in an array of key-value records. Writing such a map into
# an Avro data file is rejected, so that no data file mismatching the Iceberg schema is committed.
# Parquet and ORC data files keep the type of the keys.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}_time_map"
trap 'rm -rf "${USER_FILES_PATH:?}/${PREFIX:?}"_* 2>/dev/null' EXIT

for format in Avro Parquet ORC; do
    echo "--- ${format} ---"
    table="${PREFIX}_${format}"
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} (m Map(Time64(6), Int64), a Array(Map(Time, String)))
        ENGINE = IcebergLocal('${USER_FILES_PATH}/${table}/', '${format}')"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        INSERT INTO ${table} VALUES (map(toTime64('12:00:00.5', 6), 1), [map(toTime('13:00:00'), 'x')])" 2>&1 \
        | grep -o -m1 'NOT_IMPLEMENTED' | head -n1
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${table}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
done
