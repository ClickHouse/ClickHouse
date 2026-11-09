#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# A tuple element of an Iceberg or Paimon table must be readable from data files written before a schema change,
# whether it is selected, filtered in `WHERE`, read together with a `PREWHERE`, or used by a row policy.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_tuple_element"
rm -rf "${ICEBERG_PATH}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_ice (id Int64, s Tuple(a Int64, b Nullable(Int64), c Tuple(x Int64)), arr Array(Tuple(y Int64)))
        ENGINE = IcebergLocal('${ICEBERG_PATH}');
    INSERT INTO t_ice VALUES (1, (10, 100, (1000)), [(7)]), (2, (20, NULL, (2000)), [(8), (9)]);
    ALTER TABLE t_ice ADD COLUMN extra Nullable(Int64);
    INSERT INTO t_ice VALUES (3, (30, 300, (3000)), [], 5);
    SELECT id, s.a, s.b, s.c.x, arr.y FROM t_ice ORDER BY id;
    SELECT id FROM t_ice WHERE s.a = 20;
    SELECT id, s.a FROM icebergLocal('${ICEBERG_PATH}') ORDER BY id;
"

# A `PREWHERE` on a schema-changed file is replayed as a `FilterTransform` after the schema transform.
# It can name top-level columns only: a tuple element in `PREWHERE` is `ILLEGAL_PREWHERE` for this engine.
# `parallel_replicas_for_cluster_engines = 0`: the cluster wrapper of the engine does not accept `PREWHERE`.
${CLICKHOUSE_CLIENT} --parallel_replicas_for_cluster_engines=0 --query "
    SELECT id, s.a FROM t_ice PREWHERE id >= 2 ORDER BY id;
    SELECT id FROM t_ice WHERE s.b = 100;
    CREATE ROW POLICY policy_05616 ON t_ice USING s.a >= 20 TO ALL;
    SELECT id FROM t_ice ORDER BY id;
    SELECT id FROM t_ice PREWHERE id < 3 ORDER BY id;
    DROP ROW POLICY policy_05616 ON t_ice;
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --parallel_replicas_for_cluster_engines=0 --query "
    ALTER TABLE t_ice RENAME COLUMN s TO r;
    SELECT id, r.a FROM t_ice ORDER BY id;
    SELECT id, r.a FROM t_ice PREWHERE id = 1;
    DROP TABLE t_ice;
"

rm -rf "${ICEBERG_PATH}"

# Paimon matches top-level columns by field id: the first file of `paimon_row_rename` was written before
# `s` was renamed to `r`, so it is read under its own names. Per-run copy: the flaky check runs this test
# concurrently with itself.
PAIMON_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${PAIMON_PATH}"
mkdir -p "${PAIMON_PATH}"
cp -r "${CUR_DIR}/data_minio/paimon_row_rename/" "${PAIMON_PATH}/"

${CLICKHOUSE_CLIENT} --enable_paimon_storage_engine=1 --query "
    CREATE TABLE t_paimon ENGINE = PaimonLocal('${PAIMON_PATH}/paimon_row_rename');
    SELECT id, r.a, r.b FROM t_paimon ORDER BY id;
    CREATE ROW POLICY policy_05616_paimon ON t_paimon USING r.a >= 20 TO ALL;
    SELECT id, r.b FROM t_paimon ORDER BY id;
    DROP ROW POLICY policy_05616_paimon ON t_paimon;
    DROP TABLE t_paimon;
"

rm -rf "${PAIMON_PATH}"
