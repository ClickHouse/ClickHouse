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

# Paimon matches top-level columns by field id: the first data file is written under schema 0, before `s` was
# renamed to `r`, so it is read under its own names.
PAIMON_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/paimon_row_rename"
rm -rf "${PAIMON_PATH}"
mkdir -p "${PAIMON_PATH}"/{schema,snapshot,manifest,bucket-0}

for schema in '0 s' '1 r'; do
    read -r id name <<< "${schema}"
    echo '{"version":3,"id":'"${id}"',"highestFieldId":3,"partitionKeys":[],"primaryKeys":[],"options":{},"timeMillis":0,"fields":[{"id":0,"name":"id","type":"BIGINT"},{"id":1,"name":"'"${name}"'","type":{"type":"ROW","fields":[{"id":2,"name":"a","type":"BIGINT"},{"id":3,"name":"b","type":"BIGINT"}]}}]}' > "${PAIMON_PATH}/schema/schema-${id}"
done
echo -n '1' > "${PAIMON_PATH}/snapshot/LATEST"
echo '{"id":1,"schemaId":1,"baseManifestList":"manifest-list-1","deltaManifestList":"manifest-list-1","commitUser":"test","commitIdentifier":0,"commitKind":"APPEND","timeMillis":0}' > "${PAIMON_PATH}/snapshot/snapshot-1"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PAIMON_PATH}/bucket-0/data-0.parquet', 'Parquet', 'id Nullable(Int64), s Tuple(a Nullable(Int64), b Nullable(Int64))')
        VALUES (1, (10, 100)), (2, (20, NULL));
    INSERT INTO FUNCTION file('${PAIMON_PATH}/bucket-0/data-1.parquet', 'Parquet', 'id Nullable(Int64), r Tuple(a Nullable(Int64), b Nullable(Int64))')
        VALUES (3, (30, 300));
"

SIZE_0=$(wc -c < "${PAIMON_PATH}/bucket-0/data-0.parquet")
SIZE_1=$(wc -c < "${PAIMON_PATH}/bucket-0/data-1.parquet")
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PAIMON_PATH}/manifest/manifest-list-1', 'Avro',
        '_FILE_NAME String, _FILE_SIZE Int64, _NUM_ADDED_FILES Int64, _NUM_DELETED_FILES Int64,
         _PARTITION_STATS Tuple(_MAX_VALUES String, _MIN_VALUES String, _NULL_COUNTS Array(Int64)), _SCHEMA_ID Int64')
        VALUES ('manifest-1', 1, 2, 0, ('', '', []), 1);
    INSERT INTO FUNCTION file('${PAIMON_PATH}/manifest/manifest-1', 'Avro',
        '_KIND Int32, _PARTITION String, _BUCKET Int32, _TOTAL_BUCKETS Int32,
         _FILE Tuple(_FILE_NAME String, _FILE_SIZE Int64, _ROW_COUNT Int64, _MIN_KEY String, _MAX_KEY String,
             _KEY_STATS Tuple(_MAX_VALUES String, _MIN_VALUES String, _NULL_COUNTS Array(Int64)),
             _VALUE_STATS Tuple(_MAX_VALUES String, _MIN_VALUES String, _NULL_COUNTS Array(Int64)),
             _MIN_SEQUENCE_NUMBER Int64, _MAX_SEQUENCE_NUMBER Int64, _SCHEMA_ID Int64, _LEVEL Int32,
             _EXTRA_FILES Array(String), _CREATION_TIME Nullable(DateTime64(6)), _DELETE_ROW_COUNT Nullable(Int64),
             _EMBEDDED_FILE_INDEX Nullable(String), _FILE_SOURCE Nullable(Int8), _VALUE_STATS_COLS Array(Int64))')
        VALUES
            (0, unhex('00000000'), 0, 1, ('data-0.parquet', ${SIZE_0}, 2, '', '', ('', '', []), ('', '', []), 0, 0, 0, 0, [], NULL, NULL, NULL, NULL, [])),
            (0, unhex('00000000'), 0, 1, ('data-1.parquet', ${SIZE_1}, 1, '', '', ('', '', []), ('', '', []), 0, 0, 1, 0, [], NULL, NULL, NULL, NULL, []));
"

${CLICKHOUSE_CLIENT} --enable_paimon_storage_engine=1 --query "
    CREATE TABLE t_paimon ENGINE = PaimonLocal('${PAIMON_PATH}');
    SELECT id, r.a, r.b FROM t_paimon ORDER BY id;
    CREATE ROW POLICY policy_05616_paimon ON t_paimon USING r.a >= 20 TO ALL;
    SELECT id, r.b FROM t_paimon ORDER BY id;
    DROP ROW POLICY policy_05616_paimon ON t_paimon;
    DROP TABLE t_paimon;
"

rm -rf "${PAIMON_PATH}"
