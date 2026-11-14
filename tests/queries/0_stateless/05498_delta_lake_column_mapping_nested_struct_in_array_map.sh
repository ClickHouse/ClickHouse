#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# With column mapping, the fields of a struct inside an array or a map are stored under their physical names.
# They were read as empty arrays and default values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CUR_DIR}/data_delta_lake/column_mapping_nested"

for mode in id name
do
    echo "--- column mapping mode ${mode}"
    TABLE="deltaLakeLocal('${DATA_DIR}/mode_${mode}/delta')"
    EXPECTED="file('${DATA_DIR}/mode_${mode}/expected/*.parquet', Parquet)"

    # `clickhouse-local` allows reading only inside the current directory.
    (cd "${DATA_DIR}" && ${CLICKHOUSE_LOCAL} -q "
    SELECT ByteType, array_of_structs, map_of_rows, nested_struct FROM ${TABLE} ORDER BY ByteType;
    SELECT ByteType, array_of_structs.ab, mapValues(map_of_rows).ab FROM ${TABLE} ORDER BY ByteType;
    SELECT count() FROM ${TABLE} WHERE has(array_of_structs.ab, 3);
    SELECT
        (SELECT count() FROM (SELECT * FROM ${TABLE} EXCEPT ALL SELECT * FROM ${EXPECTED})),
        (SELECT count() FROM (SELECT * FROM ${EXPECTED} EXCEPT ALL SELECT * FROM ${TABLE}));
    ")
done
