#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - no-parallel-replicas: see 04147_iceberg_orc_schema_evolution_row_policy.sh.
#
# Tuple elements inside arrays (`arr.u` of `arr Array(Tuple(u ..., v ...))`) read from Iceberg Parquet
# files with field ids, where the Iceberg column mapper names the list element `arr.element`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_tuple_in_array"
TEST_TABLE="t_ice_tuple_in_array_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TEST_TABLE}" > /dev/null 2>&1 || true
    rm -rf "${ICEBERG_PATH}"
}
trap cleanup EXIT
rm -rf "${ICEBERG_PATH}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    DROP TABLE IF EXISTS ${TEST_TABLE};
    CREATE TABLE ${TEST_TABLE}
    (
        id Int64,
        arr Array(Tuple(u Nullable(Int64), v Nullable(String))),
        arrn Array(Tuple(u Int64, v String)),
        deep Array(Tuple(s Tuple(p Nullable(Int64), q Nullable(String)), w Nullable(Int64))),
        aa Array(Array(Tuple(u Nullable(Int64), v Nullable(String)))),
        st Tuple(x Array(Tuple(u Nullable(Int64), v Nullable(String))), y Nullable(Int64))
    )
    ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');

    INSERT INTO ${TEST_TABLE} SELECT number,
        [(number, 'v'), (number + 1, NULL)],
        [(number, 'v'), (number + 1, 'w')],
        [((number, 'q'), number * 10)],
        [[(number, 'a')], [(number + 100, 'b')]],
        ([(number, 'x')], number)
    FROM numbers(3);
"

for prewhere in 0 1; do
    echo "optimize_move_to_prewhere = ${prewhere}"
    ${CLICKHOUSE_CLIENT} --optimize_move_to_prewhere="${prewhere}" --query "
        SELECT arr.u FROM ${TEST_TABLE} ORDER BY arr.u;
        SELECT id, arr.u, arr.v FROM ${TEST_TABLE} ORDER BY id;
        SELECT id, arr.u, arr FROM ${TEST_TABLE} ORDER BY id;
        SELECT id FROM ${TEST_TABLE} WHERE has(arr.u, 2) ORDER BY id;
        SELECT id, arrn.u, deep.s.p, deep.s, deep.w, aa.u, st.x.u FROM ${TEST_TABLE} ORDER BY id;
    "
done

echo "icebergLocal"
${CLICKHOUSE_CLIENT} --query "SELECT arr.v FROM icebergLocal('${ICEBERG_PATH}') ORDER BY arr.v"
