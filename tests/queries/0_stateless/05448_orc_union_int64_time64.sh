#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires the ORC format
#
# A time of day is written to ORC as a `long` marked with `iceberg.long-type` = `TIME`, an `Int64` or
# `UInt64` as a plain `long`. A `Variant` of both must be writable, and both branch orders of the union
# (`Variant` sorts its alternatives) must be read back with each branch matched to its own alternative.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_orc_union_time.orc"

for types in "Int64 Time64(6)" "UInt64 Time64(6)" "Int64 Time"
do
    read -r int_type time_type <<< "${types}"
    variant="Variant(${int_type}, ${time_type})"
    echo "${variant}"
    ${CLICKHOUSE_LOCAL} --enable_time_time64_type=1 --query "
        SELECT number AS n,
               multiIf(n % 3 = 0, (n * 1000)::${int_type}::${variant}, n % 3 = 1, '12:34:56.123456'::Time64(6)::${time_type}::${variant}, NULL) AS v
        FROM numbers(6) FORMAT ORC" > "${DATA_FILE}"
    ${CLICKHOUSE_LOCAL} --enable_time_time64_type=1 --query "
        DESCRIBE file('${DATA_FILE}') FORMAT TSV;
        SELECT n, v, variantType(v) FROM file('${DATA_FILE}') ORDER BY n;
        SELECT n, v, variantType(v) FROM file('${DATA_FILE}', ORC, 'n UInt64, v Variant(UInt64, Time64(3))') ORDER BY n;"
done

rm -f "${DATA_FILE}"
