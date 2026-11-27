#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires the ORC format
#
# An ORC `long` with the Iceberg attribute `iceberg.long-type` = `TIME` stores microseconds since midnight.
# Read as an integer of any width, also inside `Nullable` or `LowCardinality`, a tuple or an array,
# it is the stored microseconds, the same as for `Int64`, and not the seconds of an intermediate `Time64(6)`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_TMP}/05872_orc_time_of_day_integer_targets"
mkdir -p "${DATA_DIR}"
file="${DATA_DIR}/time.orc"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        toTime64('12:34:56.123456', 6) AS t,
        tuple(toTime64('12:34:56.123456', 6)) AS tup,
        [toTime64('12:34:56.123456', 6)] AS arr
    FORMAT ORC" > "${file}"

for type in Int64 UInt64 Int128 UInt128 Int256 UInt256 'Nullable(Int128)' 'LowCardinality(Int64)' 'LowCardinality(Nullable(Int64))'; do
    ${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types 1 --query "SELECT toTypeName(t), t FROM file('${file}', ORC, 't ${type}')"
done

${CLICKHOUSE_LOCAL} --query "SELECT tup, arr FROM file('${file}', ORC, 'tup Tuple(Int128), arr Array(Int128)')"
${CLICKHOUSE_LOCAL} --allow_suspicious_low_cardinality_types 1 --query "SELECT tup, arr FROM file('${file}', ORC, 'tup Tuple(LowCardinality(Int64)), arr Array(LowCardinality(Int64))')"

# A time type is still read as a time.
${CLICKHOUSE_LOCAL} --query "SELECT toTypeName(t), t FROM file('${file}', ORC, 't Nullable(Time64(6))')"
${CLICKHOUSE_LOCAL} --query "SELECT toTypeName(t), t FROM file('${file}', ORC, 't Time64(3)')"

rm -rf "${DATA_DIR}"
